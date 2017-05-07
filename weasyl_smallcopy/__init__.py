import psycopg2
import sys
import time

steps = []


def step(name):
	def wrapper(func):
		steps.append((name, func))

	return wrapper


@step("initialize schema")
def schema_init(cur, **config):
	with open("schema.sql", "r") as f:
		schema_init_sql = f.read()

	cur.execute(schema_init_sql)
	cur.execute("SET search_path = public")


@step("alembic_version")
def copy_alembic_version(cur, **config):
	cur.execute("INSERT INTO smallcopy.alembic_version SELECT * FROM alembic_version")


@step("login")
def copy_login(cur, *, staff, **config):
	cur.execute(
		"INSERT INTO smallcopy.login (userid, login_name, last_login, settings, email) "
		"SELECT userid, login_name, 0, settings, login_name || '@weasyl.com' FROM login WHERE userid = ANY (%(staff)s)",
		{"staff": staff})


@step("authbcrypt")
def copy_authbcrypt(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.authbcrypt (userid, hashsum) "
		"SELECT userid, '$2a$12$qReI924/8pAsoHu6aRTX2ejyujAZ/9FiOOtrjczBIwf8wqXAJ22N.' FROM authbcrypt INNER JOIN smallcopy.login USING (userid)")


@step("character")
def copy_character(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.character (charid, userid, unixtime, char_name, age, gender, height, weight, species, content, rating, settings, page_views)
		SELECT charid, userid, unixtime, char_name, age, gender, height, weight, species, content, rating, character.settings, page_views
		FROM character
			INNER JOIN smallcopy.login USING (userid)
		WHERE
			rating <= 20 AND
			character.settings !~ '[hf]'
	""")


@step("charcomment")
def copy_charcomment(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.charcomment (commentid, userid, targetid, parentid, content, unixtime, indent, settings, hidden_by)
		WITH RECURSIVE t AS (
			SELECT commentid, charcomment.userid, targetid, parentid, charcomment.content, charcomment.unixtime, indent, charcomment.settings, hidden_by
			FROM charcomment
				INNER JOIN smallcopy.character ch ON targetid = charid
				INNER JOIN smallcopy.login ccu ON charcomment.userid = ccu.userid
				INNER JOIN smallcopy.login chu ON ch.userid = chu.userid
			WHERE
				parentid = 0 AND
				charcomment.settings !~ '[hs]'
			UNION SELECT charcomment.commentid, charcomment.userid, charcomment.targetid, charcomment.parentid, charcomment.content, charcomment.unixtime, charcomment.indent, charcomment.settings, charcomment.hidden_by
			FROM charcomment
				INNER JOIN t ON charcomment.parentid = t.commentid
				INNER JOIN smallcopy.login ccu ON charcomment.userid = ccu.userid
			WHERE charcomment.settings !~ '[hs]'
		)
			SELECT * FROM t
	""")


@step("folder")
def copy_folder(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.folder (folderid, parentid, userid, title, settings) "
		"SELECT folderid, parentid, userid, title, folder.settings FROM folder INNER JOIN smallcopy.login USING (userid)")


@step("submission")
def copy_submission(cur, *, staff, **config):
	cur.execute(
		"INSERT INTO smallcopy.submission (submitid, folderid, userid, unixtime, title, content, subtype, rating, settings, page_views, sorttime, fave_count) "
		"SELECT submitid, folderid, userid, unixtime, title, content, subtype, rating, settings, page_views, sorttime, fave_count FROM submission "
		"WHERE userid = ANY (%(staff)s) AND rating <= 20 AND settings !~ '[hf]'",
		{"staff": staff})


@step("collection")
def copy_collection(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.collection (userid, submitid, unixtime, settings)
		SELECT collection.userid, submitid, collection.unixtime, collection.settings
		FROM collection
			INNER JOIN submission USING (submitid)
		WHERE
			collection.userid = ANY (%(staff)s) AND
			submission.userid = ANY (%(staff)s) AND
			submission.rating <= 20 AND
			submission.settings !~ '[hf]'
	""", {"staff": staff})


@step("comments")
def copy_comments(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.comments (commentid, userid, target_user, target_sub, parentid, content, unixtime, indent, settings, hidden_by)
		WITH RECURSIVE t AS (
			SELECT commentid, comments.userid, target_user, target_sub, parentid, comments.content, comments.unixtime, indent, comments.settings, hidden_by
			FROM comments
				LEFT JOIN submission ON target_sub = submitid
				LEFT JOIN login ON target_user = login.userid
			WHERE
				comments.userid = ANY (%(staff)s) AND
				(submission.userid IS NULL OR (
					submission.userid = ANY (%(staff)s) AND
					submission.rating <= 20 AND
					submission.settings !~ '[hf]')) AND
				(login.userid IS NULL OR login.userid = ANY (%(staff)s)) AND
				parentid IS NULL AND
				comments.settings !~ '[hs]'
			UNION SELECT comments.commentid, comments.userid, comments.target_user, comments.target_sub, comments.parentid, comments.content, comments.unixtime, comments.indent, comments.settings, comments.hidden_by
			FROM comments
				INNER JOIN t ON comments.parentid = t.commentid
			WHERE
				comments.userid = ANY (%(staff)s) AND
				comments.settings !~ '[hs]'
		)
			SELECT * FROM t
	""", {"staff": staff})


@step("commishclass")
def copy_commishclass(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.commishclass (classid, userid, title) "
		"SELECT classid, userid, title FROM commishclass INNER JOIN smallcopy.login USING (userid)")


@step("commishdesc")
def copy_commishdesc(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.commishdesc (userid, content) "
		"SELECT userid, content FROM commishdesc INNER JOIN smallcopy.login USING (userid)")


@step("commishprice")
def copy_commishprice(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.commishprice (priceid, classid, userid, title, amount_min, amount_max, settings) "
		"SELECT priceid, classid, userid, title, amount_min, amount_max, commishprice.settings FROM commishprice INNER JOIN smallcopy.login USING (userid)")


@step("cron_runs")
def copy_cron_runs(cur, **config):
	cur.execute("INSERT INTO smallcopy.cron_runs (last_run) SELECT last_run FROM cron_runs")


@step("journal")
def copy_journal(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.journal (journalid, userid, title, rating, unixtime, settings, page_views)
		SELECT journalid, userid, title, rating, unixtime, settings, page_views
		FROM journal
		WHERE
			userid = ANY (%(staff)s) AND
			rating <= 20 AND
			settings !~ '[hf]'
	""", {"staff": staff})


@step("favorite")
def copy_favorite(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.favorite (userid, targetid, type, unixtime, settings)
		SELECT favorite.userid, targetid, type, favorite.unixtime, favorite.settings
		FROM favorite
			INNER JOIN smallcopy.login fu ON favorite.userid = fu.userid
			INNER JOIN profile ON favorite.userid = profile.userid
			LEFT JOIN smallcopy.submission ON favorite.type = 's' AND favorite.targetid = submission.submitid
			LEFT JOIN smallcopy.character ON favorite.type = 'f' AND favorite.targetid = character.charid
			LEFT JOIN smallcopy.journal ON favorite.type = 'j' AND favorite.targetid = journal.journalid
		WHERE profile.config !~ '[hv]' AND (
			submitid IS NOT NULL OR
			charid IS NOT NULL OR
			journalid IS NOT NULL)
	""")


@step("frienduser")
def copy_frienduser(cur, *, staff, **config):
	cur.execute(
		"INSERT INTO smallcopy.frienduser (userid, otherid, settings, unixtime) "
		"SELECT userid, otherid, settings, unixtime FROM frienduser WHERE userid = ANY (%(staff)s) AND otherid = ANY (%(staff)s) AND position('p' in settings) = 0",
		{"staff": staff})


@step("google_doc_embeds")
def copy_google_doc_embeds(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.google_doc_embeds (submitid, embed_url)
		SELECT submitid, embed_url
		FROM google_doc_embeds
			INNER JOIN submission USING (submitid)
		WHERE
			userid = ANY (%(staff)s) AND
			rating <= 20 AND
			settings !~ '[hf]'
	""", {"staff": staff})


@step("journalcomment")
def copy_journalcomment(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.journalcomment (commentid, userid, targetid, parentid, content, unixtime, indent, settings, hidden_by)
		WITH RECURSIVE t AS (
			SELECT commentid, journalcomment.userid, targetid, parentid, content, journalcomment.unixtime, indent, journalcomment.settings, hidden_by
			FROM journalcomment
				INNER JOIN journal ON targetid = journalid
			WHERE
				journalcomment.userid = ANY (%(staff)s) AND
				journal.userid = ANY (%(staff)s) AND
				journal.rating <= 20 AND
				journal.settings !~ '[hf]' AND
				parentid = 0 AND
				position('h' in journalcomment.settings) = 0
			UNION SELECT journalcomment.commentid, journalcomment.userid, journalcomment.targetid, journalcomment.parentid, journalcomment.content, journalcomment.unixtime, journalcomment.indent, journalcomment.settings, journalcomment.hidden_by
			FROM journalcomment
				INNER JOIN t ON journalcomment.parentid = t.commentid
			WHERE
				journalcomment.userid = ANY (%(staff)s) AND
				position('h' in journalcomment.settings) = 0
		)
			SELECT * FROM t
	""", {"staff": staff})


@step("profile")
def copy_profile(cur, *, staff, **config):
	cur.execute("""
		INSERT INTO smallcopy.profile (userid, username, full_name, catchphrase, artist_type, unixtime, profile_text, settings, stream_url, page_views, config, jsonb_settings, stream_time, stream_text)
		SELECT
			userid, username, full_name, catchphrase, artist_type, unixtime, profile_text, settings,
			stream_url, page_views,
			regexp_replace(config, '[map]|^(?=[^map]*$)', ('{"",m,a,p}'::text[])[('x' || md5(username))::bit(8)::integer %% 4 + 1]),
			jsonb_settings, stream_time, stream_text
		FROM profile
		WHERE userid = ANY (%(staff)s)
	""", {"staff": staff})


@step("searchtag")
def copy_searchtag(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.searchtag (tagid, title)
		SELECT tagid, title
		FROM (
			SELECT tagid FROM searchmapchar INNER JOIN smallcopy.character ON targetid = charid
			UNION SELECT tagid FROM searchmapjournal INNER JOIN smallcopy.journal ON targetid = journalid
			UNION SELECT tagid FROM searchmapsubmit INNER JOIN smallcopy.submission ON targetid = submitid
		) AS t
			INNER JOIN searchtag USING (tagid)
	""")


@step("searchmapchar")
def copy_searchmapchar(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.searchmapchar (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapchar.settings FROM searchmapchar INNER JOIN smallcopy.character ON targetid = charid")


@step("searchmapjournal")
def copy_searchmapjournal(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.searchmapjournal (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapjournal.settings FROM searchmapjournal INNER JOIN smallcopy.journal ON targetid = journalid")


@step("searchmapsubmit")
def copy_searchmapsubmit(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.searchmapsubmit (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapsubmit.settings FROM searchmapsubmit INNER JOIN smallcopy.submission ON targetid = submitid")


@step("submission_tags")
def copy_submission_tags(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.submission_tags (submitid, tags) "
		"SELECT submitid, tags FROM submission_tags INNER JOIN smallcopy.submission USING (submitid)")


@step("siteupdate")
def copy_siteupdate(cur, *, staff, **config):
	cur.execute(
		"INSERT INTO smallcopy.siteupdate (updateid, userid, title, content, unixtime) "
		"SELECT updateid, userid, title, content, unixtime FROM siteupdate WHERE userid = ANY (%(staff)s)",
		{"staff": staff})


@step("tag_updates")
def copy_tag_updates(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.tag_updates (updateid, submitid, userid, added, removed, updated_at) "
		"SELECT updateid, submitid, tag_updates.userid, added, removed, updated_at FROM tag_updates INNER JOIN smallcopy.submission USING (submitid) INNER JOIN smallcopy.login ON tag_updates.userid = login.userid")


@step("user_links")
def copy_user_links(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.user_links (linkid, userid, link_type, link_value) "
		"SELECT linkid, userid, link_type, link_value FROM user_links INNER JOIN smallcopy.login USING (userid)")


@step("user_streams")
def copy_user_streams(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.user_streams (userid, start_time, end_time) "
		"SELECT userid, start_time, end_time FROM user_streams INNER JOIN smallcopy.login USING (userid)")


@step("user_timezones")
def copy_user_timezones(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.user_timezones (userid, timezone) "
		"SELECT userid, timezone FROM user_timezones INNER JOIN smallcopy.login USING (userid)")


@step("useralias")
def copy_useralias(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.useralias (userid, alias_name, settings) "
		"SELECT userid, alias_name, useralias.settings FROM useralias INNER JOIN smallcopy.login USING (userid)")


@step("userinfo")
def copy_userinfo(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.userinfo (userid, birthday, gender, country) "
		"SELECT userid, 0, gender, country FROM userinfo INNER JOIN smallcopy.login USING (userid)")


@step("userpremium")
def copy_userpremium(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.userpremium (userid, unixtime, terms) "
		"SELECT userid, unixtime, terms FROM userpremium INNER JOIN smallcopy.login USING (userid)")


@step("userstats")
def copy_userstats(cur, **config):
	cur.execute(
		"INSERT INTO smallcopy.userstats (userid, page_views, submit_views, followers, faved_works, journals, submits, characters, collects, faves) "
		"SELECT userid, page_views, submit_views, followers, faved_works, journals, submits, characters, collects, faves FROM userstats INNER JOIN smallcopy.login USING (userid)")


@step("watchuser")
def copy_watchuser(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.watchuser (userid, otherid, settings, unixtime)
		SELECT watchuser.userid, otherid, watchuser.settings, unixtime
		FROM watchuser
			INNER JOIN smallcopy.login u USING (userid)
			INNER JOIN smallcopy.login o ON otherid = o.userid
	""")


@step("add necessary media entries")
def copy_media(cur, **config):
	cur.execute("""
		INSERT INTO smallcopy.media (mediaid, media_type, file_type, attributes, sha256)
		WITH RECURSIVE t AS (
			SELECT mediaid FROM submission_media_links
				INNER JOIN smallcopy.submission USING (submitid)
			UNION SELECT mediaid FROM user_media_links
				INNER JOIN smallcopy.login USING (userid)
			UNION SELECT described_with_id FROM media_media_links
				INNER JOIN t ON describee_id = mediaid
		)
			SELECT mediaid, media_type, file_type, attributes, sha256 FROM t INNER JOIN media USING (mediaid)
	""")

	cur.execute("""
		INSERT INTO smallcopy.submission_media_links (linkid, mediaid, submitid, link_type, attributes)
		SELECT linkid, mediaid, submitid, link_type, attributes FROM submission_media_links
			INNER JOIN smallcopy.submission USING (submitid)
	""")

	cur.execute("""
		INSERT INTO smallcopy.user_media_links (linkid, mediaid, userid, link_type, attributes)
		SELECT linkid, mediaid, userid, link_type, attributes FROM user_media_links
			INNER JOIN smallcopy.login USING (userid)
	""")

	cur.execute("""
		INSERT INTO smallcopy.disk_media (mediaid, file_path, file_url)
		SELECT mediaid, file_path, file_url FROM disk_media
			INNER JOIN smallcopy.media USING (mediaid)
	""")

	cur.execute("""
		INSERT INTO smallcopy.media_media_links (linkid, described_with_id, describee_id, link_type, attributes)
		WITH RECURSIVE t AS (
			SELECT mediaid FROM submission_media_links
				INNER JOIN smallcopy.submission USING (submitid)
			UNION SELECT mediaid FROM user_media_links
				INNER JOIN smallcopy.login USING (userid)
			UNION SELECT described_with_id FROM media_media_links
				INNER JOIN t ON describee_id = mediaid
		)
			SELECT linkid, described_with_id, describee_id, link_type, attributes
			FROM media_media_links
				INNER JOIN t ON describee_id = t.mediaid
	""")


@step("update sequences")
def update_sequences(cur, **config):
	sequences = [
		("ads", "id"),
		("character", "charid"),
		("charcomment", "commentid"),
		("comments", "commentid"),
		("commishclass", "classid"),
		("commishprice", "priceid"),
		("emailblacklist", "id"),
		("folder", "folderid"),
		("journal", "journalid"),
		("journalcomment", "commentid"),
		("login", "userid"),
		("media_media_links", "linkid"),
		("media", "mediaid"),
		("message", "noteid"),
		("oauth_bearer_tokens", "id"),
		("report", "reportid"),
		("reportcomment", "commentid"),
		("searchtag", "tagid"),
		("siteupdate", "updateid"),
		("submission_media_links", "linkid"),
		("submission", "submitid"),
		("tag_updates", "updateid"),
		("user_links", "linkid"),
		("user_media_links", "linkid"),
		("welcome", "welcomeid"),
	]

	cur.execute("SELECT 'smallcopy.' || sequence_name FROM information_schema.sequences WHERE sequence_schema = 'smallcopy'")
	database_sequences = frozenset(name for name, in cur)

	cur.execute(
		"SELECT pg_get_serial_sequence('smallcopy.' || table_name::text, column_name::text) "
		"FROM UNNEST (%(sequences)s) AS t (table_name unknown, column_name unknown)",
		{'sequences': sequences},
	)
	updating_sequences = frozenset(name for name, in cur)

	if not database_sequences <= updating_sequences:
		missing = sorted(database_sequences - updating_sequences)
		raise RuntimeError(f"Sequences missing update: {missing!r}")

	for table, column in sequences:
		cur.execute(
			"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), COALESCE((SELECT max({column}) + 1 FROM {table}), 1), false)"
			.format(table="smallcopy." + table, column=column))


def main(config):
	max_name_length = max(len(name) for name, func in steps)
	time_format_string = "\x1b[u\x1b[32m✓\x1b[0m {:%d} \x1b[2m{:6.2f}s\x1b[0m" % (max_name_length,)
	overall_start = time.perf_counter()

	with psycopg2.connect(**config["database"]) as db, db.cursor() as cur:
		for name, func in steps:
			print("\x1b[s… " + name, end="", file=sys.stderr, flush=True)

			step_start = time.perf_counter()
			func(cur, **config)
			step_time = time.perf_counter() - step_start

			print(time_format_string.format(name, step_time), file=sys.stderr, flush=True)

	db.close()

	overall_time = time.perf_counter() - overall_start
	print(" " * (max_name_length + 3) + "───────")
	print(" " * (max_name_length + 3) + "{:6.2f}s".format(overall_time))
