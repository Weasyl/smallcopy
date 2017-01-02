PG_DATABASE=weasyl
PG_USER=weasyl
PATCH_FLAGS=--no-backup-if-mismatch

schema.sql: schema.patch
	pg_dump --no-owner --schema-only --exclude-schema=smallcopy --dbname=$(PG_DATABASE) --username=$(PG_USER) --file=$@
	< $< patch $(PATCH_FLAGS)

clean:
	rm -f schema.sql

.PHONY: clean
