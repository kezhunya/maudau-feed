# Maudau Feed

Этот репозиторий генерирует файл `update_maudau.xml` для MAUDAU.

## Файлы

- `update_maudau.py` - основной скрипт генерации фида.
- `.github/workflows/update-maudau-feed.yml` - автозапуск по расписанию + ручной запуск.
- `update_maudau.xml` - итоговый файл (создается скриптом).

## Что делает скрипт

1. Скачивает 2 XML:
   - основной: `https://aqua-favorit.com.ua/content/export/b0026fd850ce11bb0cb7610e252d7dae.xml`
   - Rozetka: `http://parser.biz.ua/Aqua/api/export.aspx?action=rozetka&key=ui82P2VotQQamFTj512NQJK3HOlKvyv7`
2. Находит товар как в прошлом скрипте: `param Артикул` -> `vendorCode` -> `offer@id`.
3. Удаляет из основного фида все, чего нет в Rozetka, кроме вендоров `Мойдодыр` и `Dusel`.
4. Обновляет из Rozetka: `price`, `old_price`, `available`.
5. Нормализует под MAUDAU (`name_ru/name_ua`, `description_ru/description_ua`, `old_price`, валидный `id`, удаляются только пустые `param`).
6. Удаляет офферы без обязательных полей MAUDAU.

## Telegram

Скрипт отправляет уведомление в Telegram (успех/ошибка), если заданы секреты:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Telegram Webhook (мгновенные кнопки)

Добавлены 4 кнопки:
- `Обновить MAUDAU`
- `Обновить EPICENTER`
- `Обновить HOTLINE`
- `Обновить ROZETKA`

Файлы webhook:
- `webhook/app.py`
- `requirements-webhook.txt`
- `render.yaml`
- `set_telegram_webhook.py`

### Быстрый запуск на Render

1. В Render: `New +` -> `Blueprint` -> выберите репозиторий `kezhunya/maudau-feed`.
2. После создания сервиса в переменные окружения добавьте:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
   - `GH_DISPATCH_TOKEN`
3. Возьмите URL сервиса, например `https://marketplace-updater-webhook.onrender.com`.
4. Установите webhook:

```bash
cd "/Users/Kezhunya/Documents/New project"
export TELEGRAM_BOT_TOKEN="..."
python3 set_telegram_webhook.py --url "https://marketplace-updater-webhook.onrender.com/telegram/webhook" --drop-pending
```

5. Для безопасности (рекомендуется): передайте `TELEGRAM_WEBHOOK_SECRET` из Render в команду:

```bash
python3 set_telegram_webhook.py --url "https://marketplace-updater-webhook.onrender.com/telegram/webhook" --secret "ВАШ_СЕКРЕТ_ИЗ_RENDER"
```

6. После переключения webhook старый polling-workflow `.github/workflows/telegram-feed-control.yml` оставлен только для ручного fallback.

### Если HOTLINE/ROZETKA отвечают 401

Webhook поддерживает авторизацию для прямых URL по env-переменным в Render.

Для каждого фида выберите тип:
- `*_AUTH_TYPE=none`
- `*_AUTH_TYPE=basic`
- `*_AUTH_TYPE=bearer`
- `*_AUTH_TYPE=header`
- `*_AUTH_TYPE=cookie`

HOTLINE:
- `HOTLINE_FEED_URL`
- `HOTLINE_AUTH_TYPE`
- для `basic`: `HOTLINE_BASIC_USER`, `HOTLINE_BASIC_PASS`
- для `bearer`: `HOTLINE_BEARER_TOKEN`
- для `header`: `HOTLINE_HEADER_NAME`, `HOTLINE_HEADER_VALUE`
- для `cookie`: `HOTLINE_COOKIE`

ROZETKA:
- `ROZETKA_FEED_URL`
- `ROZETKA_AUTH_TYPE`
- для `basic`: `ROZETKA_BASIC_USER`, `ROZETKA_BASIC_PASS`
- для `bearer`: `ROZETKA_BEARER_TOKEN`
- для `header`: `ROZETKA_HEADER_NAME`, `ROZETKA_HEADER_VALUE`
- для `cookie`: `ROZETKA_COOKIE`

## Публикация через GitHub Pages

После первого успешного workflow файл будет доступен по адресу:
`https://kezhunya.github.io/maudau-feed/update_maudau.xml`

В `Settings -> Pages` выставьте:
- `Source`: `Deploy from a branch`
- `Branch`: `main`
- `Folder`: `/ (root)`
