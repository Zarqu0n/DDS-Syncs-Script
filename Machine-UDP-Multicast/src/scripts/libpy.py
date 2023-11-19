values_dictionary = {"key1":"123", "key2":"123"}
added_values = values_dictionary
def setvalue(key,value):
    global added_values
    global values_dictionary
    values_dictionary.update({key:value})
    added_values.update({key:value})


def getvalue():
    return values_dictionary
    

def getaddedvalues():
    __temp = added_values
    resetvalues()
    return __temp

def resetvalues():
    global added_values
    added_values = {}

def subtract_dicts(dict1, dict2):
    result = {key: dict1[key] for key in dict1 if key not in dict2}
    return result
