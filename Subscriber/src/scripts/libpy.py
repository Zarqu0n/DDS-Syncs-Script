values_dictionary = {"key1":"123", "key2":"123"}
added_values = values_dictionary

def setvalue(key,value):
    global values_dictionary
    added_values.update({key:value})
    values_dictionary.update({key:value})

def getvalue():
    return values_dictionary
    

def getaddedvalues():
    __temp = added_values
    resetvalues()
    return __temp

def resetvalues():
    global added_values
    added_values = {}