from libpy import setvalue,getvalue,getaddedvalues
def main():
    setvalue("key8","8888888")
    try:
        print(getvalue()["script3"])
    except:
        pass
    #print("getting:",getvalue())
