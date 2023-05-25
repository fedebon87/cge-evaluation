'''
Il programma genera reportistica sul COVID recuperando i dati da un dataset
online, chiedendo all'utente il formato del report (txt o html)
Rimuovendo la richiesta dell'input e imponendo il formato - basta dichiarare
l'estensione in forma di stringa nella variabile output_ext -, si può usare
questo programma per la generazione periodica e automatica di reportistica.
Le dipendenze sono ridotte alle librerie più comuni nell'analisi dei dati e
a moduli presenti nella libreria standard.
'''

import io
import base64
import subprocess, os, platform, webbrowser
from threading import Thread
from queue import Queue
import pandas as pd
from matplotlib.pyplot import gcf as gcf


'''
Funzionalità del programma
'''

def choose_ext():
    '''
    Chiede all'utente l'estensione del file di report
    '''
    while(True): 
        output_ext = input("\nScegliere il formato del report. Digitare txt\
 (senza immagini) o html (con immagini): ")
        if output_ext.lower() in ("txt","html"):
            return output_ext

def choose_launch():
    '''
    Chiede all'utente se aprire il report appena generato
    '''
    while(True):
        launch_flag = input("\nVuoi aprire il report generato? Digita 'Y' per sì,\
 'N' per no: ")
        if launch_flag.lower() in ("y","n"):
            return launch_flag
        
def choose():
    '''
    Raccoglie choose_ext e choose_launch

    '''
    return (choose_ext(),choose_launch())
    
def readable(l, min_width=80, sep=", ", end="\n"):
    '''
    Prende un tipo convertibile a lista (di stringhe) e restituisce una stringa
    ben stampabile.
    '''
    l = list(l)
    c_counter = 0
    c_list = []
    s = 0
    for word in l:
        s += len(word)+len(sep)
        c_counter += len(word)+len(sep) 
        if s > min_width:
            c_list.append(c_counter)
            c_counter += len(end)
            s = 0
    sentence = sep.join(l)
    for i in c_list:
        sentence = sentence[:i]+end+sentence[i:]
    return sentence

def nicer(fl_num):
    '''
    Prende un float e restituisce una stringa ben leggibile, un po' più carina
    '''
    return f"{fl_num:,.0f}" if fl_num == int(fl_num) else f"{fl_num:,.2f}"
    

def fig_to_base64(fig):
    '''
    Converte un'immagine a stringa, non serve salvarla in un file esterno.
    fig è una figure pyplot

    '''
    img = io.BytesIO()
    fig.savefig(img, format='png', bbox_inches='tight')
    img.seek(0)
    return base64.b64encode(img.getvalue())


'''
Metriche statistiche sui casi COVID e sulle vaccinazioni per continente 
'''

def cases_per_cont(df, *args):
    '''
    Prende il dataframe, un numero variabile di stringhe (continenti) e restituisce
    minimo, massimo, media e percentuale sul totale mondiale di casi di COVID
    rispetto ai continenti in input
    '''
    data = df.loc[:,["continent","new_cases"]]
    #data = df.loc[:,["continent","location","new_cases"]] #usare col doppio criterio
    criteria = data.continent.isin(continents) #| data.location.isin(continents)
    #la seconda clausola include alcuni dati ridondanti, uso sconsigliato
    data = data.loc[criteria]
    world_total = int(data.new_cases.sum())
    data = data.groupby("continent").agg(["min","max","mean", lambda x:
                                          round(x.sum()/world_total*100,2)])
    data.columns = data.columns.set_levels(['min','max','mean','percentage'],
                                           level=1)
    return data.loc[[x for x in args],:]

def vaxx_per_cont(df, *args):
    '''
    Prende il dataframe, un numero variabile di stringhe (continenti) e restituisce
    minimo, massimo, media e percentuale sul totale mondiale di vaccinaazioni
    contro il COVID rispetto ai continenti in input
    '''
    data = df.loc[:,["continent","new_vaccinations"]]
    #data = df.loc[:,["continent","location","new_vaccinations"]] #usare col doppio criterio
    criteria = data.continent.isin(continents) #| data.location.isin(continents)
    #la seconda clausola include alcuni dati ridondanti, uso sconsigliato
    data = data.loc[criteria]
    world_total = int(data.new_vaccinations.sum())
    data = data.groupby("continent").agg(["min","max","mean", lambda x:
                                          round(x.sum()/world_total*100,2)])
    data.columns = data.columns.set_levels(['min','max','mean','percentage'],
                                           level=1)
    return data.loc[[x for x in args],:]
    
def cont_stats(df, metric,*args):
    '''
    Restituisce metriche statistiche sul dataframe in input raggruppando per un
    numero variabile di continenti passati come stringhe. Il parametro metric
    è una stringa "cases" o "vaxx", fornisce dati sui nuovi casi di COVID o
    sulle vaccinazioni.
    '''
    return cases_per_cont(df,*args) if metric=="cases" else vaxx_per_cont(df,*args)

def general_timestats(df,freq="Y"):
    '''
    Restituisce informazioni su nuovi casi e vaccinati, raggruppati per continente
    e stato, al variare di una frequenza temporale, passata come stringa per
    input. Default è "Y", per anno. Per le stringhe delle frequenze si fa
    riferimento agli offset aliases di pandas.
    '''
    data = df.loc[:,["continent","location","date","new_cases",
                     "new_vaccinations"]]
    data = data.loc[~data.location.isin(continents)]
    data["date"]=pd.to_datetime(data["date"])
    data.new_cases = data.new_cases.fillna(0).astype(pd.Int64Dtype.type,copy=True)
    data.new_vaccinations = data.new_vaccinations.fillna(0).astype(
        pd.Int64Dtype.type,copy=True)
    data.date = data.date.dt.to_period(freq=freq)
    data = data.groupby(["continent","date","location"]).sum()
    data = data.reset_index()
    data_cases = data.drop("new_vaccinations", axis=1)
    data_vaxx = data.drop("new_cases", axis=1)
    cases_min = data_cases.loc[data_cases.groupby(
        ["continent","date"])["new_cases"].idxmin()]
    cases_max = data_cases.loc[data_cases.groupby(
        ["continent","date"])["new_cases"].idxmax()]
    vaxx_min = data_vaxx.loc[data_vaxx.groupby(
        ["continent","date"])["new_vaccinations"].idxmin()]
    vaxx_max = data_vaxx.loc[data_vaxx.groupby(
        ["continent","date"])["new_vaccinations"].idxmax()]   
    data = [cases_min, cases_max, vaxx_min, vaxx_max]
    for d in data:
        d.set_index(["continent","date","location"],inplace=True)
    cases_min.rename({"new_cases":"min new_cases"},axis=1,inplace=True)
    cases_max.rename({"new_cases":"max new_cases"},axis=1,inplace=True)
    vaxx_min.rename({"new_vaccinations":"min new_vaccinations"},axis=1,inplace=True)
    vaxx_max.rename({"new_vaccinations":"max new_vaccinations"},axis=1,inplace=True)
    return data


''' 
Acquisizione del dataset e inizializzazione: threading
'''

que = Queue()
second_thread = Thread(daemon=True, target=lambda q: q.put(choose()),args=(que,))
second_thread.start()

w_path = r"https://covid.ourworldindata.org/data/owid-covid-data.csv"
#se già in possesso, inserire il path locale del dataset qui sotto al posto di w_path
path = w_path #il programma funziona offline, il dataset perde l'aggiornamento
try :
    df = pd.read_csv(path, low_memory=False)
except Exception:
    raise Exception("\nControlla la connesione a Internet.")
second_output = que.get()
output_ext = second_output[0]
launch_flag= second_output[1]
df.style.set_properties(**{'text-align': 'right'})
pd.set_option('display.width', 400)
pd.set_option('colheader_justify', 'center')
pd.set_option("display.html.border", 3)


'''
Titolo del report
'''

ord_d= df.date.sort_values(ascending=False)
last_update = ord_d.iloc[0] #data in formato B (giapponese)
del ord_d
title = "Report sul Covid aggiornato in data {}\n\n".format(
    last_update[-2:]+"-" + last_update[5:7] + "-" + last_update[:4])


'''
Descrizione del dataset
'''

buffer = io.StringIO()
df.info(verbose=False, buf=buffer)
fsize = buffer.getvalue().split("memory usage: ")[-1][:-1]
col_names = df.columns
not_null_elems = df.describe().loc["count",:].sum()
db_info = "Informazioni sul database.\n\nIl database è reperibile all'indirizzo\
 {}\nConsta di {} colonne e {:,} righe, per un totale di {:,} elementi, di cui \
{:,.0f} non nulli ({:4.2f}%).\nOccupa in memoria {}. Le intestazioni delle \
colonne sono, nell'ordine, le seguenti:\n\n".format(w_path, df.shape[1], 
df.shape[0],df.size,not_null_elems, not_null_elems/df.size*100, fsize)

    
'''
Distribuzione dei casi per continente
'''

cont_cases = df.loc[:,["new_cases","continent"]].groupby(by="continent").sum()
continents = tuple(cont_cases.index) #usata dalle funzioni statistiche
cases_distrib = "\n\nEcco un elenco della distribuzione dei casi di contagio \
per continente.\n\n"
cases_dati = ""
for i in range(len(cont_cases)):
    cases_dati += "\t"+list(cont_cases.index)[i].ljust(20)+\
                    nicer(cont_cases.new_cases[i]).rjust(20)+"\n"    
cases_dati += "\n\t"+"Totale".ljust(20)+\
                nicer(cont_cases.sum()[0]).rjust(20)+"\n"  
if output_ext.lower() == "html":
    cont_cases.plot(kind="pie",subplots=True,legend=False,ylabel="",
                    figsize=(7,7),cmap="turbo")
    fig = gcf()
    cases_pie = fig_to_base64(fig)
    casesp_code = '<img src="data:image/png;base64, {}">'.format(
        cases_pie.decode('utf-8')) + "<br>"*2
    buffer = io.StringIO()
    cont_cases.loc["Total"]=cont_cases.new_cases.sum()
    cont_cases.new_cases = cont_cases.new_cases.apply(nicer)
    #con .astype() in Windows sono convertiti a int32 per un bug documentato
    cont_cases.to_html(buf=buffer)
    cases_dati = "<br>"*2 + buffer.getvalue() + "<br>"*2
    

'''
Distribuzione delle vaccinazioni per continente
'''

cont_vaxx = df.loc[:,["new_vaccinations","continent"]].groupby(by="continent").sum()
vaxx_distrib = "\n\nEcco un elenco della distribuzione delle vaccinazioni\
 per continente.\n\n"
vaxx_dati = ""
for i in range(len(cont_vaxx)):
    vaxx_dati += "\t"+list(cont_vaxx.index)[i].ljust(20)+\
                    nicer(cont_vaxx.new_vaccinations[i]).rjust(20)+"\n"    
vaxx_dati += "\n\t"+"Totale".ljust(20)+\
                nicer(cont_vaxx.sum()[0]).rjust(20)+"\n"  
if output_ext.lower() == "html":
    cont_vaxx.plot(kind="pie",subplots=True,legend=False,ylabel="",
                    figsize=(7,7),cmap="turbo")
    fig = gcf()
    vaxx_pie = fig_to_base64(fig)
    vaxxp_code = '<img src="data:image/png;base64, {}">'.format(
       vaxx_pie.decode('utf-8')) 
    buffer = io.StringIO()
    cont_vaxx.loc["Total"]=cont_vaxx.new_vaccinations.sum()
    cont_vaxx.new_vaccinations = cont_vaxx.new_vaccinations.apply(nicer)
    cont_vaxx.to_html(buf=buffer)
    vaxx_dati = buffer.getvalue() + "<br>"*2
 
    
'''
Focus su tre continenti
'''

focus_cont = ("Europe","Oceania","South America")
focus = "\n\n\nSi comparano ora i dati per i tre continenti richiesti: {}, {}, {}.\n\n"
focus = focus.format(focus_cont[0],focus_cont[1],focus_cont[2])
focus_cases = cont_stats(df, "cases","Europe","Oceania","South America")
focus_vaxx = cont_stats(df, "vaxx","Europe","Oceania","South America")
comment = "I dati in esame confermano alcune intuizioni abbastanza naturali.\
 Il virus si è diffuso maggiormente nelle zone del pianeta ad alta intensità di\
 attività economica e ad alta densità abitativa, mentre zone pur popolate ma con\
 meno interscambio sono state meno afflitte. Va considerata la possibilità che stati\
 ricchi e con democrazie più sviluppate abbiano condotto politiche di monitoraggio\
 più affidabili. La scarsa densità abitativa e l'insularità hanno ridotto la diffusione\
 del virus, come suggeriscono i dati sull'Oceania. Quanto alle vaccinazioni, più\
 utile sarebbe studiare una serie storica, per vedere se i vaccini, che ora sembrano\
 equidistribuiti, si siano prima diffusi nei paesi più ricchi. Il confronto diretto\
 tra Europa e Sud America suggerisce che a una maggior diffusione del virus non \
 corrisponda una maggior intensità di somministrazione dei vaccini."
note = "Nota: i valori dei minimi appaiono strani. Osservando il caso inglese si\
 nota qualche non congruità del database, che riporta i casi per United Kingdom ma \
presenta anche voci per England, tutte nulle. Raccomandata analisi sulla base dati."

if  output_ext.lower() == "txt":
    focus_cases = focus_cases.apply(lambda x: x.apply(nicer))
    focus_vaxx = focus_vaxx.applymap(nicer)
    comment = readable(comment.split(), sep=" ")
    note = readable(note.split(), sep=" ")
    focus += "\nQuesto è il confronto sui nuovi contagi\n\n" + focus_cases.to_string() +\
        "\n\nQuesto è il confronto sulle vaccinazioni\n\n" + focus_vaxx.to_string() +\
            "\n\n\n" + comment + "\n\n" + note
elif output_ext.lower() == "html":
    comment = readable(comment.split(),min_width=160, sep=" ", end="<br>")
    note = readable(note.split(), min_width=160, sep=" ", end="<br>")
    
    focus_cases.plot(kind="bar", legend=False, figsize=(20,10), logy=True,
                    title= "New vaccinations", xlabel="",ylabel="",cmap="Pastel2")
    fig = gcf()
    fig.legend(["min","max","mean","percentage"],loc="right")
    focus_cases_bar = fig_to_base64(fig)
    focus_cases_code = '<img src="data:image/png;base64, {}">'.format(
        focus_cases_bar.decode('utf-8')) 
    focus_cases = focus_cases.apply(lambda x: x.apply(nicer))
    buffer = io.StringIO()
    focus_cases.to_html(buf=buffer)
    focus_cases =  "<br>"*3 + "Questo è il confronto sui nuovi contagi" +"<br>"*2 +\
        buffer.getvalue() + "<br>"*2 + focus_cases_code
    focus_vaxx.plot(kind="bar", legend=False, figsize=(20,10), logy=True,
                    title= "New vaccinations", xlabel="",ylabel="",cmap="Pastel2")
    fig = gcf()
    fig.legend(["min","max","mean","percentage"],loc="right")
    focus_vaxx_bar = fig_to_base64(fig)
    focus_vaxx_code = '<img src="data:image/png;base64, {}">'.format(
        focus_vaxx_bar.decode('utf-8')) 
    focus_vaxx = focus_vaxx.applymap(nicer)
    buffer = io.StringIO()
    focus_vaxx.to_html(buf=buffer)
    focus_vaxx = "<br>"*3 + "Questo è il confronto sulle vaccinazioni" + "<br>"*2 +\
        buffer.getvalue() + "<br>"*2 + focus_vaxx_code
    focus += focus_cases + focus_vaxx + "<br>"*3 + comment + "<br>"*2 + note
 
    
'''
Serie temporale
'''

closing = "\n\n\n\n\nIn chiusura, una serie temporale con i dati di massimo,\
 per nuovi contagi e vaccinazioni, per tutti i continenti."
closing_stats_c = general_timestats(df)[1] 
closing_stats_v = general_timestats(df)[3] 

if  output_ext.lower() == "txt":
    closing_stats_c = closing_stats_c.apply(lambda x: x.apply(nicer))
    closing_stats_v = closing_stats_v.applymap(nicer)
    closing += "\n\n\n" + closing_stats_c.to_string() +\
        "\n\n\n" + closing_stats_v.to_string() + "\n\n"
elif output_ext.lower() == "html":
    buffer_c = io.StringIO()
    buffer_v = io.StringIO()
    closing_stats_c = closing_stats_c.apply(lambda x: x.apply(nicer))
    closing_stats_v = closing_stats_v.applymap(nicer)
    closing_stats_c.to_html(buf=buffer_c)
    closing_stats_v.to_html(buf=buffer_v)
    closing += "<br>"*3 + buffer_c.getvalue() + "<br>"*3 + buffer_v.getvalue()       
    
'''
Creazione del file di report
'''

if output_ext.lower() == "html":
    title = "<h1 style='text-align:center;'> <font size='+20'><b>" +\
        title.replace("\n","<br>")+"</b></font></h1>"
        
    # è necessario sistemare il file .html per un bug in .to_html() con le tabelle
    db_info = "<p style='text-align:left;'>" + db_info.replace("\n","<br>") + "</p>"
    col_names = "<br>" + readable(col_names,min_width=160, end="<br>") + "<br>"
    cases_distrib =  "<br>"*3 + cases_distrib.replace("\n","") +"<br>"
    vaxx_distrib = "<br>"*3 + vaxx_distrib.replace("\n","") +"<br>"
    vaxx_dati = "<br>"*2 + vaxx_dati.replace("\n","")
    focus = "<br>"*5 +focus.replace("\n","")+"<br>"
    closing ="<br>"*3 +closing.replace("\n","")+"<br>"
    head = "<!DOCTYPE html> <html><title> Report </title> <body style='\
background-color:powderblue;'> "
    report = head + title + db_info + col_names + cases_distrib +\
        cases_dati + casesp_code + vaxx_distrib + vaxx_dati + vaxxp_code +\
            focus + closing + "</body></html>"
  
else :    
    report = title + db_info + readable(col_names) + cases_distrib + cases_dati +\
        vaxx_distrib + vaxx_dati + focus + closing

filename = "{}_covid_report.{}".format(last_update,output_ext.lower())
f = open(filename, "w")
f.write(report)
f.close()


'''
Lancio del file di report
'''

if launch_flag.lower() == "y":
    if output_ext.lower() == "html":
        webbrowser.open(os.path.realpath(filename))
    else :
        if platform.system() == "Windows":     #Windows
            os.startfile(os.path.realpath(filename))
        elif platform.system() == "Darwin":    #MacOS
            subprocess.call(("open", os.path.realpath(filename)))
        else:                                  #Linux
            subprocess.call(("xdg-open", os.path.realpath(filename)))


