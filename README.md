## Installation

```shellsession
$ pip install -e .
```


## Configuration

Configuration is read from a `config.json` in the working directory. Its options are:

 - **`database`**

    A PostgreSQL DSN or dict of parameters [as accepted by psycopg2][psycopg2-connect].

 - **`maximum_rating`**

    The maximum rating of content to export. One of:

     - `general`
     - `moderate`
     - `mature`
     - `explicit`

 - **`include`**

    The users whose data should be exported. Either a list of ids or the string `"all"` to export data for all users.


## Usage

```shellsession
$ pg_dump --file=schema.sql --no-owner --schema-only --schema=public --dbname=$weasyl_db --username=$weasyl_user
$ POSIXLY_CORRECT= patch schema.sql schema.patch
$ python -m weasyl_smallcopy
```


  [psycopg2-connect]: http://initd.org/psycopg/docs/module.html#psycopg2.connect
