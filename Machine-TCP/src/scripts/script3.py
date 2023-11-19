from libpy import setvalue,getvalue,getaddedvalues
x = 0
def main():
    global x
    x +=1
    setvalue("script3",str(x))