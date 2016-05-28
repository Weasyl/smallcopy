import psycopg2
import sys
import time

db = psycopg2.connect(database="weasyl", user="weasyl")

staff = [
	3,
	5,
	1014,
	1019,
	2008,
	2011,
	2061,
	2252,
	2402,
	5173,
	5756,
	15224,
	15712,
	20418,
	23613,
	34165,
	38623,
	61554,
	89199,
]

with open("schema.sql", "r") as f:
	schema_init_sql = f.read()

steps = []

def step(name):
	def wrapper(func):
		steps.append((name, func))

	return wrapper

@step("initialize schema")
def schema_init(cur):
	cur.execute(schema_init_sql)
	cur.execute("SET search_path = public")

@step("login")
def copy_login(cur):
	cur.execute(
		"INSERT INTO smallcopy.login (userid, login_name, last_login, settings, email) "
		"SELECT userid, login_name, 0, settings, login_name || '@weasyl.com' FROM login WHERE userid = ANY (%(staff)s)",
		{"staff": staff})

@step("authbcrypt")
def copy_authbcrypt(cur):
	cur.execute(
		"INSERT INTO smallcopy.authbcrypt (userid, hashsum) "
		"SELECT userid, '$2a$12$qReI924/8pAsoHu6aRTX2ejyujAZ/9FiOOtrjczBIwf8wqXAJ22N.' FROM authbcrypt INNER JOIN smallcopy.login USING (userid)")

@step("character")
def copy_character(cur):
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
def copy_charcomment(cur):
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
def copy_folder(cur):
	cur.execute(
		"INSERT INTO smallcopy.folder (folderid, parentid, userid, title, settings) "
		"SELECT folderid, parentid, userid, title, folder.settings FROM folder INNER JOIN smallcopy.login USING (userid)")

@step("submission")
def copy_submission(cur):
	cur.execute(
		"INSERT INTO smallcopy.submission (submitid, folderid, userid, unixtime, title, content, subtype, rating, settings, page_views, sorttime, fave_count) "
		"SELECT submitid, folderid, userid, unixtime, title, content, subtype, rating, settings, page_views, sorttime, fave_count FROM submission "
		"WHERE userid = ANY (%(staff)s) AND rating <= 20 AND settings !~ '[hf]'",
		{"staff": staff})

@step("collection")
def copy_collection(cur):
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
def copy_comments(cur):
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
def copy_commishclass(cur):
	cur.execute(
		"INSERT INTO smallcopy.commishclass (classid, userid, title) "
		"SELECT classid, userid, title FROM commishclass INNER JOIN smallcopy.login USING (userid)")

@step("commishdesc")
def copy_commishdesc(cur):
	cur.execute(
		"INSERT INTO smallcopy.commishdesc (userid, content) "
		"SELECT userid, content FROM commishdesc INNER JOIN smallcopy.login USING (userid)")

@step("commishprice")
def copy_commishprice(cur):
	cur.execute(
		"INSERT INTO smallcopy.commishprice (priceid, classid, userid, title, amount_min, amount_max, settings) "
		"SELECT priceid, classid, userid, title, amount_min, amount_max, commishprice.settings FROM commishprice INNER JOIN smallcopy.login USING (userid)")

@step("drop commission")
def drop_commission(cur):
	cur.execute("DROP TABLE smallcopy.commission")

@step("drop composition")
def drop_composition(cur):
	cur.execute("DROP TABLE smallcopy.composition")

@step("cron_runs")
def copy_cron_runs(cur):
	cur.execute("INSERT INTO smallcopy.cron_runs (last_run) SELECT last_run FROM cron_runs")

@step("favorite")
def copy_favorite(cur):
	cur.execute("""
		INSERT INTO smallcopy.favorite (userid, targetid, type, unixtime, settings)
		SELECT userid, targetid, type, favorite.unixtime, favorite.settings
		FROM favorite
			INNER JOIN profile USING (userid)
		WHERE
			userid = ANY (%(staff)s) AND
			profile.config !~ '[hv]'
	""", {"staff": staff})

@step("frienduser")
def copy_frienduser(cur):
	cur.execute(
		"INSERT INTO smallcopy.frienduser (userid, otherid, settings, unixtime) "
		"SELECT userid, otherid, settings, unixtime FROM frienduser WHERE userid = ANY (%(staff)s) AND otherid = ANY (%(staff)s) AND position('p' in settings) = 0",
		{"staff": staff})

@step("google_doc_embeds")
def copy_google_doc_embeds(cur):
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

@step("drop ignorecontent")
def drop_ignorecontent(cur):
	cur.execute("DROP TABLE smallcopy.ignorecontent")

@step("journal")
def copy_journal(cur):
	cur.execute("""
		INSERT INTO smallcopy.journal (journalid, userid, title, rating, unixtime, settings, page_views)
		SELECT journalid, userid, title, rating, unixtime, settings, page_views
		FROM journal
		WHERE
			userid = ANY (%(staff)s) AND
			rating <= 20 AND
			settings !~ '[hf]'
	""", {"staff": staff})

@step("journalcomment")
def copy_journalcomment(cur):
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

"""
@step("drop loginaddress")
def drop_loginaddress(cur):
	cur.execute("DROP TABLE smallcopy.loginaddress")
"""

@step("drop logininvite")
def drop_logininvite(cur):
	cur.execute("DROP TABLE smallcopy.logininvite")

@step("profile")
def copy_profile(cur):
	cur.execute(
		"INSERT INTO smallcopy.profile (userid, username, full_name, catchphrase, artist_type, unixtime, profile_text, settings, stream_url, page_views, config, jsonb_settings, stream_time, stream_text) "
		"SELECT userid, username, full_name, catchphrase, artist_type, unixtime, profile_text, settings, stream_url, page_views, config, jsonb_settings, stream_time, stream_text FROM profile WHERE userid = ANY (%(staff)s)",
		{"staff": staff})

@step("searchtag")
def copy_searchtag(cur):
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
def copy_searchmapchar(cur):
	cur.execute(
		"INSERT INTO smallcopy.searchmapchar (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapchar.settings FROM searchmapchar INNER JOIN smallcopy.character ON targetid = charid")

@step("searchmapjournal")
def copy_searchmapjournal(cur):
	cur.execute(
		"INSERT INTO smallcopy.searchmapjournal (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapjournal.settings FROM searchmapjournal INNER JOIN smallcopy.journal ON targetid = journalid")

@step("searchmapsubmit")
def copy_searchmapsubmit(cur):
	cur.execute(
		"INSERT INTO smallcopy.searchmapsubmit (tagid, targetid, settings) "
		"SELECT tagid, targetid, searchmapsubmit.settings FROM searchmapsubmit INNER JOIN smallcopy.submission ON targetid = submitid")

@step("siteupdate")
def copy_siteupdate(cur):
	cur.execute(
		"INSERT INTO smallcopy.siteupdate (updateid, userid, title, content, unixtime) "
		"SELECT updateid, userid, title, content, unixtime FROM siteupdate WHERE userid = ANY (%(staff)s)",
		{"staff": staff})

@step("tag_updates")
def copy_tag_updates(cur):
	cur.execute(
		"INSERT INTO smallcopy.tag_updates (updateid, submitid, userid, added, removed, updated_at) "
		"SELECT updateid, submitid, tag_updates.userid, added, removed, updated_at FROM tag_updates INNER JOIN smallcopy.submission USING (submitid)")

@step("user_links")
def copy_user_links(cur):
	cur.execute(
		"INSERT INTO smallcopy.user_links (linkid, userid, link_type, link_value) "
		"SELECT linkid, userid, link_type, link_value FROM user_links INNER JOIN smallcopy.login USING (userid)")

@step("user_streams")
def copy_user_streams(cur):
	cur.execute(
		"INSERT INTO smallcopy.user_streams (userid, start_time, end_time) "
		"SELECT userid, start_time, end_time FROM user_streams INNER JOIN smallcopy.login USING (userid)")

@step("user_timezones")
def copy_user_timezones(cur):
	cur.execute(
		"INSERT INTO smallcopy.user_timezones (userid, timezone) "
		"SELECT userid, timezone FROM user_timezones INNER JOIN smallcopy.login USING (userid)")

@step("useralias")
def copy_useralias(cur):
	cur.execute(
		"INSERT INTO smallcopy.useralias (userid, alias_name, settings) "
		"SELECT userid, alias_name, useralias.settings FROM useralias INNER JOIN smallcopy.login USING (userid)")

@step("userinfo")
def copy_userinfo(cur):
	cur.execute(
		"INSERT INTO smallcopy.userinfo (userid, birthday, gender, country) "
		"SELECT userid, 0, gender, country FROM userinfo INNER JOIN smallcopy.login USING (userid)")

@step("userpremium")
def copy_userpremium(cur):
	cur.execute(
		"INSERT INTO smallcopy.userpremium (userid, unixtime, terms) "
		"SELECT userid, unixtime, terms FROM userpremium INNER JOIN smallcopy.login USING (userid)")

@step("userstats")
def copy_userstats(cur):
	cur.execute(
		"INSERT INTO smallcopy.userstats (userid, page_views, submit_views, followers, faved_works, journals, submits, characters, collects, faves) "
		"SELECT userid, page_views, submit_views, followers, faved_works, journals, submits, characters, collects, faves FROM userstats INNER JOIN smallcopy.login USING (userid)")

@step("watchuser")
def copy_watchuser(cur):
	cur.execute("""
		INSERT INTO smallcopy.watchuser (userid, otherid, settings, unixtime)
		SELECT watchuser.userid, otherid, watchuser.settings, unixtime
		FROM watchuser
			INNER JOIN smallcopy.login u USING (userid)
			INNER JOIN smallcopy.login o ON otherid = o.userid
	""")

@step("add necessary media entries")
def copy_media(cur):
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

max_name_length = max(len(name) for name, func in steps)
time_format_string = "\x1b[u\x1b[32m✓\x1b[0m {:%d} \x1b[2m{:6.2f}s\x1b[0m" % (max_name_length,)
overall_start = time.perf_counter()

with db:
	cur = db.cursor()

	for name, func in steps:
		print("\x1b[s… " + name, end="", file=sys.stderr, flush=True)

		step_start = time.perf_counter()
		func(cur)
		step_time = time.perf_counter() - step_start

		print(time_format_string.format(name, step_time), file=sys.stderr, flush=True)

	cur.close()

db.close()

overall_time = time.perf_counter() - overall_start
print(" " * (max_name_length + 3) + "───────")
print(" " * (max_name_length + 3) + "{:6.2f}s".format(overall_time))
