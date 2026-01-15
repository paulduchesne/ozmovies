# Ozmovies

Dataset from [Ozmovies](https://www.ozmovies.com.au/) with relevant [Wikidata](https://www.wikidata.org) identifiers.

### Queries

Are the Wikidata IDs valid? This query will return a dataframe of all entries which are no longer active, either due to redirection or deletion.

```python
import pandas
import pathlib
import requests
import time

dataframes1 = list()
dataframes2 = list()

path = pathlib.Path.cwd() / 'ozmovies.csv'
with pandas.read_csv(path, chunksize=250) as dataframe_array:
    for df in dataframe_array:
        dataframes1.append(df)
        wikidata_ids = ' '.join(['wd:'+x for x in df.wikidata.unique() if type(x) == str])
        query = '''
            select ?wikidata 
            where {
                values ?wikidata {'''+wikidata_ids+'''}
                ?wikidata wdt:P31 ?type
            } '''

        r = requests.get('https://query.wikidata.org/sparql', params={'format': 'json', 'query': query})
        dataframes2.append(pandas.DataFrame(r.json()['results']['bindings']).dropna())
        
        time.sleep(47)

d1 = pandas.concat(dataframes1, ignore_index=True)
d2 = pandas.concat(dataframes2, ignore_index=True)
d2['wikidata'] = d2['wikidata'].apply(lambda x: x['value'].split('/')[-1])
d1 = d1.loc[~d1.wikidata.isin(d2.wikidata.unique())]

print(len(d1))
d1.head()
```

Federate with Wikidata, here pulling in "Country of Origin" (P495) data.

```python
import pandas
import pathlib
import requests
import time

dataframes1 = list()
dataframes2 = list()

path = pathlib.Path.cwd() / 'ozmovies.csv'
with pandas.read_csv(path, chunksize=250) as dataframe_array:
    for df in dataframe_array:
        dataframes1.append(df)
        wikidata_ids = ' '.join(['wd:'+x for x in df.wikidata.unique() if type(x) == str])        
        query = '''
        select ?wikidata ?countryLabel  
        where {
            values ?wikidata {'''+wikidata_ids+'''}
            ?wikidata wdt:P495 ?country .
            service wikibase:label { bd:serviceParam wikibase:language "en". }  
        } '''

        r = requests.get('https://query.wikidata.org/sparql', params={'format': 'json', 'query': query})        
        dataframes2.append(pandas.DataFrame(r.json()['results']['bindings']).dropna())
        
        time.sleep(47)

d1 = pandas.concat(dataframes1, ignore_index=True)
d2 = pandas.concat(dataframes2, ignore_index=True)

d2['wikidata'] = d2['wikidata'].apply(lambda x: x['value'].split('/')[-1])
d2['countryLabel'] = d2['countryLabel'].apply(lambda x: x['value'])
d2 = d2.pivot_table(index=['wikidata'], aggfunc=lambda x: ', '.join(sorted(x.unique()))).reset_index()
d1 = pandas.merge(d1, d2, on='wikidata', how='left')

print(len(d1))
d1.head()
```

Access third-party identifiers via Wikidata, here pulling in "IMDB ID" (P345) data.

```python
import pandas
import pathlib
import requests
import time

dataframes1 = list()
dataframes2 = list()

path = pathlib.Path.cwd() / 'ozmovies.csv'
with pandas.read_csv(path, chunksize=250) as dataframe_array:
    for df in dataframe_array:
        dataframes1.append(df)
        wikidata_ids = ' '.join(['wd:'+x for x in df.wikidata.unique() if type(x) == str])        
        query = '''
            select ?wikidata ?imdb  
            where {
                values ?wikidata {'''+wikidata_ids+'''}
                ?wikidata wdt:P345 ?imdb . 
            } '''

        r = requests.get('https://query.wikidata.org/sparql', params={'format': 'json', 'query': query})        
        dataframes2.append(pandas.DataFrame(r.json()['results']['bindings']).dropna())
        
        time.sleep(47)

d1 = pandas.concat(dataframes1, ignore_index=True)
d2 = pandas.concat(dataframes2, ignore_index=True)

d2['wikidata'] = d2['wikidata'].apply(lambda x: x['value'].split('/')[-1])
d2['imdb'] = d2['imdb'].apply(lambda x: x['value'])
d2 = d2.pivot_table(index=['wikidata'], aggfunc=lambda x: ', '.join(sorted(x.unique()))).reset_index()
d1 = pandas.merge(d1, d2, on='wikidata', how='left')

print(len(d1))
d1.head()
```


