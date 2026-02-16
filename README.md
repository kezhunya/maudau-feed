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

## Публикация через GitHub Pages

После первого успешного workflow файл будет доступен по адресу:
`https://kezhunya.github.io/maudau-feed/update_maudau.xml`

В `Settings -> Pages` выставьте:
- `Source`: `Deploy from a branch`
- `Branch`: `main`
- `Folder`: `/ (root)`
