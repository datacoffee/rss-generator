import os
import re
import boto3
from datetime import datetime
from string import Template
from html import escape

NEWS_TABLE = os.environ['DYNAMO_NEWS']
CHAPTERS_LENGHT = int(os.environ['CHAPTERS_LENGHT'])
S3_PREFIX = os.environ['S3_PREFIX']
FEED_TPL = Template(open("feed.tpl").read())
EPISODE_TPL = Template(open("item.tpl").read())


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(NEWS_TABLE)
    items = table.scan()

    feed = {'build_date': datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")}
    feed['items'] = ""

    # TODO check if episode # is number!
    for record in [i for i in items['Items'] if 'meta' in i.keys() and i['meta']['published']]:
        episode = {}
        meta = record['meta']
        episode['title'] = f'{record["episode"]}. {meta["title"]}'
        episode['link'] = meta['web_url']
        episode['mp3_url'] = f'{S3_PREFIX}{record["episode"]}.mp3'
        episode['duration_seconds'] = meta['duration_seconds']
        episode['image_url'] = f'{S3_PREFIX}{record["episode"]}.png'
        episode['mp3_size_bytes'] = meta['mp3_size_bytes']
        episode['season'] = meta['season']
        episode['episode'] = record['episode']

        # construct description
        if 'description_html' in meta:
            description_html = meta['description_html']
        else:
            if 'guest' in meta:
                description_html = '<p>Гостевой эпизод</p><p><br></p>\n'
            else:
                description_html = '<p>Новостной эпизод</p><p><br></p>'
            if 'promo_text' in meta:
                description_html += f'<p>{meta["promo_text"]}</p><p><br></p>\n'
            description_html += '<p>Shownotes:</p>\n'
            prev_dttm = datetime.strptime(record['records'][-1], "%m/%d/%Y, %H:%M:%S")
            for chapter in [i for i in record['news'] if len(i['chapters'])]:
                dttm = datetime.strptime(chapter['chapters'][-1], "%m/%d/%Y, %H:%M:%S")
                time_delta = dttm - prev_dttm
                text, links = split_news(chapter['text'])
                description_html += f'<p>{format_time(time_delta)} '
                if links:
                    description_html += f'<a href="{links[0]}">{cut_text(text)}</a>'
                else:
                    description_html += cut_text(text)
                description_html += '</p>\n'
            if 'image_src' in meta:
                description_html += f'<p><br></p>\n<p><a href="{meta["image_src"]}">Обложка</a></p>\n'
            description_html += '<p>Сайт: <a href="https://datacoffee.link/">https://datacoffee.link</a>, '
            description_html += 'канал в Telegram: <a href="https://t.me/datacoffee">https://t.me/datacoffee</a>, '
            description_html += 'профиль в Twitter: <a href="https://twitter.com/_DataCoffee_">https://twitter.com/_DataCoffee_</a></p>'
        episode['description_html'] = description_html

        episode['description_escaped'] = escape(episode['description_html'])
        episode['guid'] = meta['guid']
        episode['pub_date'] = datetime.fromtimestamp(meta['pub_date_epoch']).strftime("%a, %d %b %Y %H:%M:%S GMT")
        feed['items'] += EPISODE_TPL.substitute(episode)

    return {
        'statusCode': 200,
        'body': FEED_TPL.substitute(feed)
    }


def cut_text(text):
    if len(text) > CHAPTERS_LENGHT:
        text = text[:CHAPTERS_LENGHT-3].strip() + "..."
    return text


def split_news(news_str):
    links = re.findall(r'(https?://[^\s]+)', news_str)
    text = news_str.strip()
    for link in links:
        text = text.replace(link, '')
    text = re.sub(' +', ' ', text).strip().replace('"', "'")
    text = text[:1].upper() + text[1:]
    return (text, links)


def format_time(tdelta):
    # days = tdelta.days
    hours, rem = divmod(tdelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}:00"