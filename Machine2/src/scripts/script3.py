from libpy import setvalue,getvalue,getaddedvalues
def main():

    try:
        x = int(getvalue()["script3"])
    except:
        x = 0
    x +=3
    setvalue("script3",str(x))