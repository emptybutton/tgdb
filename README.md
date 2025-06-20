# tgdb
[![CI](https://github.com/emptybutton/tgdb/actions/workflows/ci.yml/badge.svg)](https://github.com/emptybutton/tgdb/actions?query=workflow%3ACI)
[![CD](https://github.com/emptybutton/tgdb/actions/workflows/cd.yml/badge.svg)](https://github.com/emptybutton/tgdb/actions/workflows/cd.yaml)
[![GitHub Release](https://img.shields.io/github/v/release/emptybutton/tgdb?style=flat&logo=github&labelColor=%23282e33&color=%237c73ff)](https://github.com/emptybutton/tgdb/releases)
[![Wakatime](https://wakatime.com/badge/user/0d3b7ff5-0547-4323-a43e-2a7308d973a0/project/2e316b92-fcf1-44d8-ad77-6c81e23cdfe2.svg)](https://wakatime.com/badge/user/0d3b7ff5-0547-4323-a43e-2a7308d973a0/project/2e316b92-fcf1-44d8-ad77-6c81e23cdfe2)
[![Lines](https://img.shields.io/endpoint?url=https%3A%2F%2Fghloc.vercel.app%2Fapi%2Femptybutton%2Ftgdb%2Fbadge%3Ffilter%3D.py&logo=python&label=lines&color=blue)](https://github.com/search?q=repo%3Aemptybutton%2tgdb+language%3APython+&type=code)
[![codecov](https://codecov.io/gh/emptybutton/tgdb/graph/badge.svg?token=ILGHUT1KRH)](https://codecov.io/gh/emptybutton/tgdb)

РСУБД поверх Telegram.

```bash
pip install tgdb
```

```bash
docker pull n255/tgdb:0.1.0-slim
```

> [!CAUTION]
> **Не используйте этот проект с большим количеством ботов**, так как он нарушает правила Telegram и создаёт паразитическую нагрузку на его инфраструктуру.
> 
> В противном случае ваши данные могут быть удалены, а аккаунты — заблокированы. 

## Запуск

Для запуска сервера укажите путь к YAML-конфигу через переменную окружения `CONFIG_PATH`.

Пример конфигурации:
```yaml
conf:
  uvicorn:
    host: "0.0.0.0"
    port: 8000

  api:
    id: 23598539
    hash: "6d9d7305ffc6f148dab120d24541b127"

  clients:
    bots: "/etc/tgdb/clients/bots"
    userbots: "/etc/tgdb/clients/userbots"

  horizon:
    max_len: 800
    transaction:
      max_age_seconds: 30

  message_cache:
    max_len: 100_000

  heap:
    chat: -1000000000000
    page:
      max_fullness: 0.8

  relations:
    chat: -1000000000000

  buffer:
    chat: -1000000000000
    overflow:
      len: 5000
      timeout_seconds: 0.1
```

> [!IMPORTANT]
> С данной конфигурацией один сервер потребляет не более 200 МБ памяти (без учёта обработки входящих запросов).

## Обзор
`tgdb` — это СУБД, хранящая данные в Telegram-чатах и предоставляющая доступ к ним через реляционную модель с поддержкой ACID-транзакций.

Взаимодействие с Telegram происходит через ботов/юзерботов, имеющих фиксированные [лимиты](https://limits.tginfo.me/ru) на количество запросов в определённый промежуток времени. Фактически они представляют собой аренду небольшой части инфраструктуры Telegram, поэтому считаются ресурсами ввода-вывода (аналогично пропускной способности сети).

Из-за специфики лимитов операции с сообщениями имеют существенно разную стоимость по сравнению с операциями в памяти/на диске:
- Все операции выполняются за константное время (200–400 мс), но чтение значительно дороже записи, так как доступно только через юзерботов. Для регистрации юзербота требуется отдельный номер (по закону РФ физическое лицо может иметь не более 20 номеров), тогда как для обычных ботов достаточно регистрации у BotFather.
- Несмотря на сложность создания юзерботов, за один запрос можно прочитать множество сообщений. Запись новых сообщений требует отдельного запроса для каждого сообщения.
- Обновление сообщений требует предварительного поиска целевого сообщения.
- Удаление поддерживает пакетную обработку, но также требует поиска сообщений.

## Отношения
Текущая реализация позволяет создавать отношения без поддержки миграций. Ограничения включают только доменные ограничения на размер данных.

Физически кортежи не группируются по отношениям, а хранятся в едином чате (куче). Каждый кортеж размещается в отдельном сообщении (странице) размером до 4096 символов. Следовательно, невозможно создать отношение, кортежи которого не помещаются в одну страницу.

## Операции
На данный момент можно читать кортежи только до записи, а сама запись возможна только через bulk-запрос в рамках коммита.

`tgdb` не использует MVCC, поэтому невозможно сделать **Repeatable Read** в том виде, что бы он был легче **Serializable**. В этом случае может быть только три уровня изоляции:

> `n` — количество транзакций уровня изоляции.

<table>
  <tr>
    <th>Уровень изоляции</th>
    <th>Особенности</th>
    <th>Старт</th>
    <th>Коммит</th>
    <th>Откат</th>
    <th>Память</th>
  </tr>
  <tr>
    <td><b>Serializable</b></td>
    <td>Требует повторения транзакций при ошибках сериализации</td>
    <td>O(n)</td>
    <td>O(n)</td>
    <td>O(n)</td>
    <td>O(n²)</td>
  </tr>
  <tr>
    <td><b>Read Committed</b></td>
    <td>В разработке. Операции чтения могут выполняться в 3 раза дольше</td>
    <td>O(1)</td>
    <td>O(1)</td>
    <td>O(1)</td>
    <td>O(n)</td>
  </tr>
  <tr>
    <td><b>Read Uncommitted</b></td>
    <td></td>
    <td>O(1)</td>
    <td>O(1)</td>
    <td>O(1)</td>
    <td>O(n)</td>
  </tr>
</table>

> [!IMPORTANT]
> При параллельном выполнении транзакций с разными уровнями изоляции вся группа получает гарантии минимального уровня из группы.

Чтение кортежей вне транзакций эквивалентно уровню **Read Uncommitted**, но создаёт меньшую нагрузку.

Коммиты буферизируются и хранятся в отдельном чате перед конкурентной записью в кучу. При достаточном количестве ботов задержка транзакции (10 чтений + 10 записей) составляет ~3 обращения к Telegram (~0.75 с).

Буфер хранится на сервере и сохраняется в чате только при переполнении. Это гарантирует восстановление консистентного состояния после сбоев и не ограничивает сервер пропускной способностью в одно обращение к Telegram.

Этапы выполнения транзакции:
1. Старт транзакции — сетевая задержка до сервера `tgdb`
2. Чтение из кучи — сетевая задержка до `telegram`
3. Начало коммита — сетевая задержка до сервера `tgdb`
4. Ожидание переполнения буфера — `conf.buffer.overflow.timeout_seconds` (макс.)
5. Сохранение буфера в чате — сетевая задержка до `telegram`
6. Запись в кучу — сетевая задержка до `telegram`
7. Подтверждение коммита — сетевая задержка до сервера `tgdb`

## Масштабирование
На данный момент все данные хранятся в одной куче, которая может вмещать только 1 млн сообщений (после 1 млн Telegram будет удалять сообщения до 500 тыс.), что даже в случае полного заполнения страниц ~16 ГБ (включая метаданные) и сам по себе сервер однопоточный.

В таком случае нужно секционировать данные между несколькими серверами практически всегда, даже в случае одной ноды, но сейчас нет встроенных механизмов для этого.
