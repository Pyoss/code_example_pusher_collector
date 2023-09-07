<?php

class PusherState
{
    public const TESTING = 'testing';
    public const EXIT = 'exit';
    public const PROGRESS = 'progress';
    public const COLLECT_EVENTS = 'collect_events';
    public const GET_ACCOUNT_INFO = 'get_account_info';
    public const COLLECT_ENTITIES = 'collect_entities';
    public const QUEUE_ENTITIES = 'queue_entities';
    public const FILTER_EVENTS = 'filter_events';
    public const QUEUE_NOTIFICATIONS = 'queue_notifications';
    public const QUEUE_EVENTS = 'queue_events';
}

class PusherCollector extends QueuedAccount
{

    private array $request_handlers;
    private array $entity_queue;
    private array $events;
    private ChannelList $channels;
    private int $entities_limit = 50;
    private int $events_limit = 200;
    private DB $db_drive;
    private array $events_filter;
    private array $existing_ids;
    private array $account_info;

    /**
     * Алгоритм получения фильтра для сущностей, возвращает false если все необходимые сущности получены
     * @return array{'entity': string, 'ids': array}|bool
     */
    private function get_entity_filter(): array|bool
    {
        if (!$this->entity_queue) {
            return false;
        }
        reset($this->entity_queue);
        $current_entity = key($this->entity_queue);
        $current_entity_ids = &$this->entity_queue[$current_entity];
        $ids_chunk = array_splice($current_entity_ids, 0, $this->entities_limit);
        if (!$this->entity_queue[$current_entity]) {
            unset ($this->entity_queue[$current_entity]);
        }
        return ['entity' => $current_entity, 'ids' => $ids_chunk];
    }

    /***
     * @param $event
     * @return array
     * В случае если объект не принадлежит списку "контакты компании сделки покупатели"
     * мы переформируем его, вытаскивая нужные параметры из url
     */
    private function reformat_entity($event): array
    {
        $url_explode = array_filter(explode("/", $event['object']['url']));

        $event['object']['entity'] = reset($url_explode);
        $event['object']['id'] = end($url_explode);
        return $event;
    }

    protected function step_callback(): bool
    {
        $current_state = $this->state;
        $this->setState(PusherState::EXIT);

        return match ($current_state) {
            PusherState::TESTING => $this->testFunction(),
            PusherState::GET_ACCOUNT_INFO => $this->getAccountInfoStep(),
            PusherState::QUEUE_EVENTS => $this->queueEventsStep(),
            PusherState::COLLECT_EVENTS => $this->collectEventsStep(),
            PusherState::COLLECT_ENTITIES => $this->collectEntitiesStep(),
            PusherState::FILTER_EVENTS => $this->filterEventsStep(),
            PusherState::QUEUE_NOTIFICATIONS => $this->queueNotificationsStep(),
            default => false,
        };
    }

    protected function init_variables()
    {
        $this->channels = new ChannelList($this->options['channels_settings']);
        $this->entity_queue = [];
        $this->events = [];
        $this->db_drive = new DB(@CONFIG::get_db_conf('notification_pusher'));
        $this->existing_ids = [];
        $this->setState(PusherState::GET_ACCOUNT_INFO);
    }

    private function testFunction()
    {
        if ($this->request_handlers['account_info']->result()) {
            $this->setState(PusherState::PROGRESS);
        } else {
            $this->setState(PusherState::EXIT);
        }
    }

    private function getAccountInfoStep(): bool
    {
        $this->amo::$_curl->content_type = 'application/x-www-form-urlencoded';
        $this->request_handlers['account_info'] = $this->worker->__request('request', 'GET', '/private/api/v2/json/accounts/current/');
        $this->request_handlers['set_pages'] = $this->worker->__request('ajax_request', 'POST', '/ajax/lists/row_count/', http_build_query([
            'entity' => 'events',
            'row_count' => $this->events_limit,
            'catalog_id' => ''
        ]));
        $this->setState(PusherState::QUEUE_EVENTS);
        return true;
    }

    private function queueEventsStep(): bool
    {
        $this->account_info = $this->request_handlers['account_info']->result()['data']['response']['account'];
        $time_offsetted = time() + (int)explode(':', $this->account_info['timezoneoffset'])[0] * 3600;
        $this->events_filter = [
            'useFilter' => 'y',
            'json' => 1,
            'page' => 1,
            'filter_date_switch' => 'created',
            'filter' => ['event_type' => $this->channels->getEventTypes()],
            'filter_date_from' => date('d.m.Y', $time_offsetted)];
        $this->request_handlers['events'] = $this->worker->__request('ajax_request',
            'GET',
            '/ajax/events/list/?' . http_build_query($this->events_filter)
        );

        // Получаем список существующих ID событий
        $this->db_drive->query('SELECT amo_event_id FROM notification_pusher.events ORDER BY id DESC LIMIT ?', $this->events_limit);
        $results = $this->db_drive->results('id');
        if ($results) {
            $this->existing_ids = array_map(function ($result) {
                return $result->amo_event_id;
            }, $results);
        }
        $this->setState(PusherState::COLLECT_EVENTS);
        return true;
    }

    private function collectEventsStep(): bool
    {
        if ($this->request_handlers['events']) {
            $page_searched = false;
            $events = [];
            $res_events = $this->request_handlers['events']->result();
            // TODO: exit point
            if ($res_events['result']) {
                $events = $res_events['data']['response']['items'];
                foreach ($events as $event) {
                    if (in_array($event['id'], $this->existing_ids)) {
                        $page_searched = true;
                        continue;
                    }
                    //Обрабатываем объект "беседа"
                    if ($event['object']['entity'] === 'talk') {
                        $event = $this->reformat_entity($event);
                    }
                    /**
                     * @var $event array{ id: string,
                     *     date_create: string,
                     *     author: string,
                     *     object: array{
                     *      id: int,
                     *      name:string,
                     *      entity: string,
                     *      url: string,
                     *      entity_name: string,
                     *      loss_reason_id: int},
                     *     type: int,
                     *     name: string,
                     *     event: string
                     * }
                     */
                    $this->events[] = $event;
                    // Для очереди получения сущностей
                    $this->entity_queue[$event['object']['entity']][] = $event['object']['id'];
                }
                $this->events_filter['page']++;
            }

            // Выход из очереди
            if (!$events || $page_searched || count($events) < $this->events_limit) {
                $this->setState(PusherState::COLLECT_ENTITIES);
                return true;
            }
        }
        $this->request_handlers['events'] = $this->worker->__request('ajax_request',
            'GET',
            '/ajax/events/list/?' . http_build_query($this->events_filter)
        );
        $this->setState(PusherState::COLLECT_EVENTS);
        return true;
    }

    private function collectEntitiesStep(): bool
    {
        if ($this->request_handlers['entity_handler']) {
            $res_entities = $this->request_handlers['entity_handler']->result();
            // TODO: exit point
            if ($res_entities['result']) {
                $entities = array_pop($res_entities['data']['_embedded']);
                foreach ($entities as $entity) {
                    foreach ($this->events as &$event) {
                        if ($event['object']['id'] == $entity['id']) {
                            $event['object'] = array_merge($event['object'], $entity);
                        }
                    }
                }
            }
        }
        $current_entity_filter = $this->get_entity_filter();
        if (!$current_entity_filter) {
            $this->setState(PusherState::FILTER_EVENTS);
        } else {
            $this->request_handlers['entity_handler'] = $this->worker->__request('ajax_request',
                'GET',
                '/api/v4/' . $current_entity_filter['entity'] . '?' . http_build_query(
                    ['limit' => $this->entities_limit,
                        'with' => ['contacts', 'customers', 'companies', 'leads'],
                        'filter' => ['id' => $current_entity_filter['ids']]]));
            $this->setState(PusherState::COLLECT_ENTITIES);
            // TODO : Exit point
        }
        return true;
    }

    private function filterEventsStep(): bool
    {
        if (!$this->events) {
            return false;
        }
        foreach ($this->events as $event_id => &$event) {
            $channels_found = false;
            foreach ($this->channels->getChannelIdsForEvent($event) as $channel_id) {
                $event['channels'][] = $channel_id;
                $channels_found = true;
            }
            if (!$channels_found) {
                unset($this->events[$event_id]);
            }
        }
        $this->setState(PusherState::QUEUE_NOTIFICATIONS);
        return true;
    }

    private function queueNotificationsStep(): bool
    {
        foreach ($this->events as $event) {
            foreach ($event['channels'] as $channel_id) {
                $this->queueNotification($channel_id, $event);
            }
        }
        return true;

    }

    private function queueNotification(string $channel_id, array $event): void
    {
        $channel = $this->channels->getChannel($channel_id);
        $receivers = $this->channels->getReceivers($channel, $event);
        $storage_folder = CONFIG::GET('SYSTEM_STORAGE');
        $path = $storage_folder .
            $this->widget_name . DIRECTORY_SEPARATOR .
            $this->amo_account_id . DIRECTORY_SEPARATOR . $event['id'] . '.json';

        // Упаковываем событие для файла
        $responsible = 'unknown';
        foreach ($this->account_info['users'] as $user) {
            if ($event['object']['responsible_user_id'] == $user['id']) {
                $responsible = $user['name'];
            }
        }
        $event['date_create'] = str_replace('Вчера', 'Yesterday', $event['date_create']);
        $event['date_create'] = str_replace('Сегодня', 'Today', $event['date_create']);
        $event['date_create'] = strtotime($event['date_create']);

        $event_file = [
            'id' => $event['id'],
            'date_create' => date('d.m.Y G:i', $event['date_create']),
            'author' => $event['author'],
            'type' => $event['type'],
            'event_name' => $event['event'],
            'entity' => [
                'name' => $event['object']['name'],
                'id' => $event['object']['id'],
                'price' => $event['object']['price'],
                'entity' => $event['object']['entity'],
                'responsible' => $responsible
            ]
        ];
        foreach ($event['object']['custom_fields_values'] as $cfv) {
            $event_file['custom_fields'][$cfv['field_id']] = [
                'name' => $cfv['field_name'],
                'value' => is_array($cfv['values'][0]) ? $cfv['values'][0]['value'] : $cfv['values'][0]
            ];
        }
        file_put_contents($path, json_encode($event_file));
        $uploaded = $this->db_drive->query('INSERT INTO notification_pusher.events 
        (events.path, events.date, events.amo_event_id, events.amo_account_id) VALUES (?, FROM_UNIXTIME(?), ?, ?)',
            $path,
            $event['date_create'],
            $event['id'],
            $this->amo_account_id);
        if (!$uploaded) {
            $this->log('Ошибка сохранения события ' . $event['id'] . ' в базу данных. Возможно, событие уже существует.');
            unlink($path);
            return;
        }
        $id = $this->db_drive->insert_id();
        foreach ($receivers as $receiver) {

            $this->db_drive->query('INSERT INTO notification_pusher.notifications 
    (event_id, receiver_id, amo_account_id, notification_state, channel_id) VALUES (?, ?, ?, ?, ?)',
                $id, $receiver, $this->amo_account_id, 'new', $channel_id);
        }
    }

}

class ChannelList
{
    /**
     * @var array{
     *     array{
     *     name:string,
     *     event_type: array{int},
     *     filters: array,
     *     receivers: array,
     *     sound: string,
     *     sound_type: int,
     *     managers: array{groups: array, managers: array},
     *     denied_managers: array{groups: array, managers: array},
     *     id: int,
     *     active: int
     *      }
     *     } $channels
     **/
    public array $channels;

    /**
     * @return array
     */
    public function getChannel($id): array
    {
        return $this->channels[$id];
    }

    public function __construct($channel_settings)
    {
        $this->channels = (array)json_decode(base64_decode($channel_settings), true);
    }

    public function getEventTypes(): array
    {
        $event_types = [];
        foreach ($this->channels as $channel) {
            if ($channel['event_type'][0]) {
                $event_types[] = $channel['event_type'][0];
            }
        }
        return $event_types;
    }

    /**
     * Получает каналы, подходящие под событие
     * @param $event
     * @return Generator
     */
    public function getChannelIdsForEvent($event): Generator
    {
        foreach ($this->channels as $channel_id => $channel) {
            // Фильтр активности
            if (!$channel['active']) {
                continue;
            }
            // Фильтр совпадения типа
            if ($event['type'] != $channel['event_type'][0]) {
                continue;
            }
            if ($channel['filters']) {
                $object = $event['object'];
                // Фильтр совпадения сущности по фильтрам
                if ($channel['filters'][$object['entity']]['active'] != 1 && $object['entity'] != 'talk') {
                    # Тип "беседа" никогда не исключается из фильтра
                    continue;
                }
                // Фильтр совпадения сделок по воронке
                if ($object['entity'] == 'leads' && $channel['filters']['leads']['pipeline_rules']) {
                    $pipeline_rules = $channel['filters']['leads']['pipeline_rules'];
                    if (!$pipeline_rules[$object['pipeline_id']] || !in_array($object['status_id'], $pipeline_rules[$object['pipeline_id']]))
                        continue;
                }
            }
            yield $channel_id;
        }
    }

    public function getReceivers($channel, $event): array
    {
        $receivers = [];
        if (in_array('choose', $channel['receivers'])) {
            $receivers = array_merge($receivers, $channel['managers']['managers']);
        }
        if (in_array('main_user', $channel['receivers'])) {
            $receivers[] = $event['object']['responsible_user_id'];
        }
        $denied_managers = $channel['denied_managers']['managers'] ?: [];
        $receivers = array_diff($receivers, $denied_managers);
        return array_unique($receivers);
    }

}