# English

## What is it?

This is callback plugin that dumps most of the Ansible internal state to the external PostgreSQL database.

## What is this for?

If you ever had to:
* know the value of the certain variable before the role or task starts;
* implement an audit soultion which should send data to some remote storage for unchangeability;  
* investigate into some Ansible function while dreaming of looking under the hood 

...then this plugin is right for you.

## Requirements

* CentOS/RHEL 7.x;
* Ansible 2.9.x;
* Python >=3.6.x;
* PostgreSQL >=10.x (this was debugged with Postgres 12, though)

## Quick start guide

1. Copy to your project directory:
   1. playbooks/callback_plugins/logdb.py
   2. playbooks/module_utils/pg8000/*
   3. playbooks/module_utils/scramp/*
2. Add settings to the ansible.cfg as follows:


    [defaults]
    stdout_callback = log2db
    callable_plugins = log2db
    callback_whitelist = log2db

    [log2db_callback]
    host = <your PostgreSQL server hostname>
    port = <your PostgreSQL server port, usually 5432>
    user = <database account with insert privilege>
    pass = <database account password>
    db = <database name, default is "ansible">
    table = <table name, default is "logs">

3. Do not forget to setup a database and a table before the first launch. 
Also, an account with proper rights is a must:


    [user@hostname]> sudo -u postgres psql
    postgres=# CREATE DATABASE <my_db_name> WITH OWNER postgres;
    postgres=# CREATE USER <my_db_user>;
    postgres=# GRANT CONNECT, CREATE, TEMPORARY ON DATABASE <my_db_name> to <my_db_user>;
    postgres=# CREATE TABLE IF NOT EXISTS <my_table_name> (
     uuid uuid not null,
     data jsonb,
     timestamp timestamp with time zone,
     id bigserial
     constraint logs_pk
     primary key,
     origin text);
    postgres=# ALTER TABLE <my_table_name> OWNER to <my_db_account>;
    postgres=# CREATE INDEX IF NOT EXISTS <my_table_name>_uuid_index on <my_table_name> (uuid);
    postgres=# ALTER USER <my_db_user> WITH PASSWORD '<my_db_password>';


## How to send a donation to the author

If you want to thank the author - [this is a donate link](https://yoomoney.ru/to/410011277351108). Any sum is happily accepted. 

## Legal information

This project is conceived and performed by me, Sergey Pechenko, on my own will, out of working hours, using own hardware. 

Copyright: (С) 2021, Sergey Pechenko

Based on "default.py" callback plugin for Ansible, which has own copyrights:

(C) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>

(C) 2017 Ansible Project

This project uses MIT-licensed components as follows: 

* pg8000 (c) 2007-2009, Mathieu Fenniak 

* scramp (C) 2019 Tony Locke 

Portions for these components that provide possibility for Ansible to load and run them are also (C) 2021, Sergey Pechenko. 

## License

GPLv3+ (please see LICENSE)

## Contact

You can ask your questions about this plugin at the [Ansible chat](https://t.me/pro_ansible), or [PM me](https://t.me/tnt4brain) 


# Русский

## Что это?

Коллбэк-плагин для Ansible, который позволяет сохранять бОльшую часть внутренних данных Ansible во внешнюю БД.

## Зачем это?

Если тебе когда-нибудь:
* требовалось при отладке знать значение конкретной переменной перед исполнением таска или вызовом роли;
* приходилось организовывать аудит с сохранением данных в отдельном от Ansible внешнем хранилище;
* случалось разбираться с какой-то функций Ansible в мечтах о возможности "заглянуть под капот" - 

...то этот плагин - для тебя.

## Что требуется?

* CentOS/RHEL 7.x;
* Ansible 2.9.x;
* Python >=3.6.x;
* PostgreSQL >=10 (отлаживалось на 12);


## Как запустить?

1. Скопируй в каталог с проектом:
   1. playbooks/callback_plugins/logdb.py
   2. playbooks/module_utils/pg8000/*
   3. playbooks/module_utils/scramp/*
2. Укажи в ansible.cfg следующее:


    [defaults]
    stdout_callback = logdb
    callable_plugins = logdb
    callback_whitelist = logdb

    [logdb_callback]
    host = <имя хоста PostgreSQL>
    port = <порт сервера PostgreSQL, обычно 5432>
    user = <учётная запись в БД с правами на INSERT>
    pass = <пароль этой учётной записи>
    db = <название БД, по умолчанию "ansible">
    table = <название таблицы, по умолчанию "logs">
    
3. Перед запуском не забудь создать БД и таблицу. А ещё понадобится создать учётку и дать ей права:


    [user@hostname]> sudo -u postgres psql
    postgres=# CREATE DATABASE <название БД> WITH OWNER postgres;
    postgres=# CREATE USER <учётная запись>;
    postgres=# GRANT CONNECT, CREATE, TEMPORARY ON DATABASE <название БД> to <учётная запись>;
    postgres=# CREATE TABLE IF NOT EXISTS <название таблицы> (
     uuid uuid not null,
     data jsonb,
     timestamp timestamp with time zone,
     id bigserial
     constraint logs_pk
     primary key,
     origin text);
    postgres=# ALTER TABLE <название таблицы> OWNER to <учётная запись>;
    postgres=# CREATE INDEX IF NOT EXISTS <название таблицы>_uuid_index
	on <название таблицы> (uuid);
    postgres=# ALTER USER <учётная запись> WITH PASSWORD '<пароль учётной записи>';
    
## Поблагодарить автора

Если хочешь поблагодарить автора - вот [ссылка для донатов](https://yoomoney.ru/to/410011277351108). Буду рад любой сумме. 

## Правовая информация

Этот проект задуман и выполнен мною, Сергеем Печенко, по личной инициативе в нерабочее время на личном оборудовании. 

Авторские права: (С) 2021, Sergey Pechenko

Проект выполнен на основе коллбэк-плагина "default.py" для Ansible. Авторские права на оригинальный файл:

(C) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>

(C) 2017 Ansible Project


При создании проекта по лицензии MIT использованы следующие компоненты: 
* pg8000 (C) Mathieu Fenniak
* scramp (C) Tony Locke 

Авторские права на части этих компонентов, обеспечивающие Ansible возможность их загрузки и выполнения: (С) 2021, Сергей Печенко

## Лицензия

GPLv3+

## Контакты

Можешь задать свои вопросы в [чате по Ansible](https://t.me/pro_ansible), или [написать мне в ЛС](https://t.me/tnt4brain). 
