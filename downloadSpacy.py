import spacy
try:
  spacy.cli.download('en_core_web_lg')
except:
  pass
