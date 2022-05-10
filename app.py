import flask
import yake
import spacy
import json
import string
from flask import jsonify, make_response, request
from collections import Counter
from cassandraManager import CassandraManager
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation
from heapq import nlargest

nlp = spacy.load("en_core_web_lg")

categories = ["Politics", "Sports", "World", "Finance",
              "Technology", "Lifestyle", "Entertainment", "Trending", "Business"]
categoriesNlp = {}

for category in categories:
    categoriesNlp[category] = nlp(category)
'''
cassyManager = CassandraManager()
print("Cassandra Manager: fetching indexes...")
indexes = {
            "scoopt_title_index": cassyManager.get_table_indexes("scoopt_title_index"),
            "scoopt_text_index": cassyManager.get_table_indexes("scoopt_text_index")
}

print("Cassandra Manager: done fetching indexes")
print("Spacy: processing indexes...")
indexNlps = {}
for tableKey in indexes.keys():
    if(tableKey not in indexNlps.keys()):
        indexNlps[tableKey] = {}
    for sourceKey in indexes[tableKey].keys():
        if(sourceKey not in indexNlps[tableKey].keys()):
            indexNlps[tableKey][sourceKey] = {}
        print(tableKey, sourceKey)
        
        for index in indexes[tableKey][sourceKey]:
            indexNlps[tableKey][sourceKey][index] = nlp(index)
print("Spacy: done processing indexes")

def splitIndex(index):
    for name in cassyManager.sources:
        if name in index:
            return index.split(name)[0], name
    
    if(cassyManager.refresh_sources(index)):
        return splitIndex(index)

    return None, None
'''


def summarize(text, per=0.3):
    doc= nlp(text)
    tokens=[token.text for token in doc]
    word_frequencies={}
    for word in doc:
        lowerWord = word.text.lower()
        if summaryPassMap.get(lowerWord) is None:
                if word_frequencies.get(lowerWord) is not None:
                    word_frequencies[lowerWord] = 1
                else:
                    word_frequencies[lowerWord] += 1
    max_frequency=max(word_frequencies.values())
    for word in word_frequencies.keys():
        word_frequencies[word]=word_frequencies[word]/max_frequency
    sentence_tokens= [sent for sent in doc.sents]
    sentence_scores = {}
    for sent in sentence_tokens:
        for word in sent:
            lowerWord = word.text.lower()
            if word_frequencies.get(lowerWord) is not None:
                if sentence_scores.get(sent) is None:                            
                    sentence_scores[sent]=word_frequencies[word.text.lower()]
                else:
                    sentence_scores[sent]+=word_frequencies[word.text.lower()]
    select_length=int(len(sentence_tokens)*per)
    summary=nlargest(select_length, sentence_scores,key=sentence_scores.get)
    final_summary=[word.text for word in summary]
    summary=''.join(final_summary)
    return summary

def getMostSimilarWords(tableName, word, source, wordList):
    wordSimilarityList = []
    wordToken = indexNlps[tableName][source][word]
    for otherWord in wordList:
        if(otherWord == word):
            continue
        otherWordToken = indexNlps[tableName][source][otherWord]
        similarity = wordToken.similarity(otherWordToken)
        if similarity < 0.71:
            continue 
        wordSimilarityList.append(otherWord+source)
    return wordSimilarityList

def getTags(corpus, numTags):
    # kw_extractor = yake.KeywordExtractor()
    text = corpus
    language = "en"
    max_ngram_size = 1
    deduplication_threshold = 0.9
    numOfKeywords = numTags
    custom_kw_extractor = yake.KeywordExtractor(
        lan=language, n=max_ngram_size, dedupLim=deduplication_threshold, top=numOfKeywords, features=None)
    keywords = custom_kw_extractor.extract_keywords(text)
    top_keys = []
    for kw in keywords:
        top_keys.append(kw[0])
    return top_keys


def getTopCategories(tags):
    word_dict = {}
    for i in tags:
        token1 = nlp(i)
        word_dict[str(i)] = {}
        for j in categories:
            token2 = categoriesNlp[j]
            word_dict[str(i)][str(j)] = float(token1.similarity(token2))
        # print(j,i,":")
        # print(token2,token1,":",token1.similarity(token2))
    categories_count = {}
    for key in word_dict.keys():
        # print("-----------------------")
        # print("Word: ", key)
        tempa = dict(Counter(word_dict[key]).most_common(5)).keys()
        # print(tempa)
        count = 0
        # print("Categories: ")
        for k in tempa:
            if count == 3:
                break
            if k not in categories_count.keys():
                categories_count[k] = 0
            categories_count[k] += 1
            count += 1
    # for ka in categories_count.keys():
    # 	print("Category: ",ka,"| Count: ",categories_count[ka])
    ffa = 0
    toreturn = []
    for w in sorted(categories_count, key=categories_count.get, reverse=True):
        if ffa == 3:
            break
        toreturn.append(w)
        ffa += 1
    return toreturn


app = flask.Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    return "<h1>Hello!</h1>"

@app.route('/getTags', methods=['POST'])
def getFlaskTags():
    corpus = request.json.get('text')
    numTags = request.json.get('no')
    return json.dumps(getTags(corpus, numTags))

@app.route('/getCategories', methods=['POST'])
def getFlaskCategories():
    corpus = request.json.get('text')
    numTags = request.json.get('no')
    includeTags = request.json.get('include_tags')
    if(includeTags is None):
        return json.dumps(getTopCategories(getTags(corpus, numTags)))
    else:
        tags = getTags(corpus, numTags)
        return json.dumps({"tags": tags, "category": getTopCategories(tags)})

@app.route('/getSummary', methods=['POST'])
def getFlaskSummary():
    corpus = request.json.get('text')
    return summarize(corpus)
'''
@app.route('/getSimilarIndexes', methods=['POST'])
def getFlaskSimilar():
    tableName = request.json.get('table_name')
    reverseIndex = request.json.get('reverse_index')   
    word, source = splitIndex(reverseIndex)
    if(word not in indexNlps[tableName][source].keys()):
        indexes[tableName][source].append(word)
        indexNlps[tableName][source][word] = nlp(word)
    return json.dumps(getMostSimilarWords(tableName, word, source, indexes[tableName][source]))

@app.route('/normalize', methods=['POST'])
def getFlaskNormalized():
    data = request.json.get('data')
    doc = nlp(data)
    result = []
    for token in doc:
        if(nlp.vocab[token.text].is_stop == True):
            continue
        tokenText = token.text.translate(str.maketrans('', '', string.punctuation))
        if(len(tokenText) == 0):
            continue
        
        result.append(token.lemma_.lower().replace("'", "''"))
    
    return json.dumps(result)
'''

if __name__ == "__main__":
    app.run()
