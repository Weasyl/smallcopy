## Usage

```shellsession
$ pg_dump --file=schema.sql --no-owner --schema-only --schema=public --dbname=$weasyl_db --username=$weasyl_user
$ POSIXLY_CORRECT= patch schema.sql schema.patch
$ python smallcopy.py
```
