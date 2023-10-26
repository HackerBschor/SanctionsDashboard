import newspaper
from newspaper import ArticleException

import requests
from datetime import datetime

from GDELTParameters import countries, languages, sort_directions, languages_alpha2
import pandas as pd
import swifter
import json
from json import JSONDecodeError


class GDELT:
    # Specify the GDELT API endpoint
    _url = 'https://api.gdeltproject.org/api/v2/doc/doc'

    def __init__(self, query, start: [datetime, None] = None, end: [datetime, None] = None, max_records=100,
                 sort: sort_directions = "HybridRel", source_country=None, source_language=None):
        self.query = query
        self.start = start
        self.end = end
        self.max_records = max_records
        self.sort = sort
        self.source_country = source_country
        self.source_language = source_language

        self.search_query = None
        self.params = None

    def _call(self):
        response = requests.get(GDELT._url, params=self.params)

        if response.status_code == 200:
            try:
                return json.loads(response.text, strict=False)
            except JSONDecodeError as e:
                raise Exception(str(e) + "\n\n" + response.text[:min(100, len(response.text))])
        else:
            print('Request failed with status code:', response.status_code)
            return None

    def _build_query(self):
        search_query = self.query

        if self.source_country is not None:
            if self.source_country in countries:
                source_country = countries[self.source_country]
            elif self.source_country not in countries.values():
                raise ValueError("Invalid source country")
            else:
                source_country = self.source_country

            search_query += " sourcecountry:" + source_country

        if self.source_language is not None:
            if self.source_language in languages:
                source_language = languages[self.source_language]
            elif self.source_language not in languages.values():
                raise ValueError("Invalid source language")
            else:
                source_language = self.source_language

            search_query += " sourcelang:" + source_language

        self.search_query = search_query

    def _build_params(self, mode: str):
        params = {
            'query': self.search_query,
            'maxrecords': self.max_records,
            'mode': mode,
            'format': 'json',
        }

        if self.sort is None:
            sort_dir = sort_directions[0]
        elif self.sort in sort_directions:
            sort_dir = self.sort
        else:
            raise Exception("Sort direction can be: " + "," + sort_directions)

        params["sort"] = sort_dir

        if self.start is None and self.end is None:
            params["timespan"] = "FULL"
        else:
            if self.start is None or self.end is None:
                raise Exception("No Start or End date (both or nothing can be none)")

            params["STARTDATETIME"] = self.start.strftime("%Y%m%d000000")
            params['ENDDATETIME'] = self.end.strftime("%Y%m%d000000")

        self.params = params

    def get_articles(self) -> pd.DataFrame:
        self._build_query()
        self._build_params('artlist')
        result = self._call()
        df = pd.DataFrame.from_records(result["articles"])
        return df

    def get_timeline(self):
        self._build_query()
        self._build_params('timelinevolinfo')
        data = self._call()

        data = data["timeline"][0]["data"]
        df = pd.DataFrame(data)
        df["date"] = df["date"].map(lambda x: datetime.strptime(x, '%Y%m%dT%H%M%SZ'))
        df.rename(columns={"value": "intensity"}, inplace=True)
        return df

    def get_tone_chart(self):
        self._build_query()
        self._build_params('tonechart')
        data = self._call()

        return pd.DataFrame(data["tonechart"])

    @staticmethod
    def _get_parse_article(url, language):
        try:
            article = newspaper.Article(url=url, language=languages_alpha2[language])
            article.download()
            article.parse()

        except ArticleException:
            return None

        """return {
            "title": str(article.title),
            "text": str(article.text),
            "authors": article.authors,
            "published_date": str(article.publish_date),
            "top_image": str(article.top_image),
            "videos": article.movies,
            "keywords": article.keywords,
            "summary": str(article.summary)
        }"""

        return str(article.text)

    @staticmethod
    def scrape_articles(articles: pd.DataFrame, col_url="url", col_language="language", col_content="content"):
        articles[col_content] = articles.swifter.apply(lambda x: GDELT._get_parse_article(x[col_url], x[col_language]),
                                                       axis=1)


if __name__ == '__main__':
    gdelt = GDELT("embargo russia", start=datetime.strptime('2019-01-01', '%Y-%m-%d'), end=datetime.now())
    df = gdelt.get_timeline()
    df.plot(x="date", y="intensity")
    import matplotlib.pyplot as plt
    plt.show()
