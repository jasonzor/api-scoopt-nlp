import flask
import yake
import spacy
import json
import string
from flask import jsonify, make_response, request
from collections import Counter
from cassandraManager import CassandraManager

nlp = spacy.load("en_core_web_lg")

categories = ["Politics", "Sports", "World", "Finance",
              "Technology", "Lifestyle", "Entertainment", "Trending", "Business"]

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
    names = ["YOUTUBE", "REDDIT", "TWITCH", "AXIOS", "WIKIPEDIA", "TIKTOK", "DAILYMOTION"]
    for name in names:
        if name in index:
            return index.split(name)[0], name
    
    return None, None

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
            token2 = nlp(j)
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
    return json.dumps(getTopCategories(getTags(corpus, numTags)))

@app.route('/getSimilarIndexes', methods=['POST'])
def getFlaskSimilar():
    tableName = request.json.get('table_name')
    reverseIndex = request.json.get('reverse_index')   
    word, source = splitIndex(reverseIndex)
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
    

if __name__ == "__main__":
    app.run()
