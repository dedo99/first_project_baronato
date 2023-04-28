import string


def parse(text):
    new_text = text.translate(str.maketrans('', '', string.punctuation))
    return new_text.lower()
