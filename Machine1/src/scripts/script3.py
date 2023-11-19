from libpy import setvalue,getvalue,getaddedvalues
x = 0
def main():
    try:
        z = getvalue()["script3"]
    except:
        z = 0
    x = int(z) -2
    setvalue("script3",str(x))