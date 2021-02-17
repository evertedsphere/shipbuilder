from google.cloud import bigquery
from google.oauth2 import service_account
from flask import Flask, render_template

app = Flask(__name__)

credentials = service_account.Credentials.from_service_account_file('./credentials.json')
project_id = 'ship-inf'
client = bigquery.Client(credentials= credentials,project=project_id)

@app.route('/ships/<character>')
def ships_for_character(character):
    tags = "(SELECT name FROM `danbooru-data.danbooru.posts`.tags)"
    q = client.query(f"""
        WITH posts_char_count AS (
          SELECT
            `danbooru-data.danbooru.posts`.id post_id,
            COUNT(post_tag) char_count
          FROM `danbooru-data.danbooru.posts`, UNNEST(tags) AS post_tag
          WHERE post_tag.category = 4
          GROUP BY post_id),
          tag_stats AS (
          SELECT
            post_tag.name tag_name,
            COUNT(post_tag.name) tag_count
          FROM `danbooru-data.danbooru.posts`,
            UNNEST(tags) AS post_tag
          WHERE `danbooru-data.danbooru.posts`.id IN (
            SELECT post_id
            FROM posts_char_count
            WHERE char_count = 2)
            AND '{character}' IN (
            SELECT name
            FROM `danbooru-data.danbooru.posts`.tags)
            AND post_tag.category = 4
          GROUP BY tag_name
          ORDER BY tag_count DESC)
        SELECT tag_name, tag_count
        FROM tag_stats
        WHERE tag_name <> '{character}';
    """)
    results = q.result()
    rows = []
    for row in results:
        tag_name = row['tag_name'].replace("_", " ")
        tag_count = row['tag_count']
        rows.append({'name': tag_name, 'count': tag_count})
    return render_template('ships.html', rows=rows, character=character.replace("_", " "))
