import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import json
import plotly.express as px
from streamlit_option_menu import option_menu

#Chargement des données à partir d'un fichier CSV
#Fonction pour récupérer les données des stations de Vélib 
st.set_page_config(
    page_title="Statut Vélib Métropole",
    page_icon=":bike:",
    layout="wide",
    initial_sidebar_state="expanded"
)
url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"

response = requests.get(url)
if response.status_code == 200:
    data=response.content
    # D'abord, on décode les bytes en string
    json_string =data.decode('utf-8')

    # Ensuite, on transforme le string en JSON
    data = json.loads(json_string)

# Récupération des données:
station_id=[]
bikes_available=[]
docks_available=[]
for i in range(0,len(data["data"]["stations"])):
            station_id.append(data["data"]["stations"][i]["station_id"])
            bikes_available.append(data["data"]["stations"][i]["num_bikes_available"])
            docks_available.append(data["data"]["stations"][i]["num_docks_available"])
# Conversion en DataFrame    
df1=pd.DataFrame(list(zip(station_id,bikes_available,docks_available)),columns=["station_id","vélos_disponibles","places_disponibles"])

url2="https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
response2 = requests.get(url2)
if response2.status_code == 200:
    data2=response2.content
    # D'abord, on décode les bytes en string
    json_string2 =data2.decode('utf-8')

    # Ensuite, on transforme le string en JSON
    data2 = json.loads(json_string2)

station_id_list=[]
name_list=[]
lat_list=[]
lon_list=[]
capacity_list=[]
for i in range(0,len(data2["data"]["stations"])):
    station_id_list.append(data2["data"]["stations"][i]["station_id"])
    name_list.append(data2["data"]["stations"][i]["name"])
    lat_list.append(data2["data"]["stations"][i]["lat"])
    lon_list.append(data2["data"]["stations"][i]["lon"])
    capacity_list.append(data2["data"]["stations"][i]["capacity"])
# Conversion en DataFrame    
df2=pd.DataFrame(list(zip(station_id_list,name_list,lat_list,lon_list,capacity_list)),columns=["station_id","name","lat","lon","capacity"])



df=pd.merge(df1,df2,on='station_id')
with st.sidebar:choose = option_menu("Menu", ["trouves la station la plus proche", "Choisis ta station"])

if choose=="trouves la station la plus proche":
    # Affichage des données dans Streamlit
    st.title("Statut des Stations Vélib Métropole")
    st.markdown("""
    Cette application fournit un aperçu en temps réel de la disponibilité des vélos et des places dans les stations Vélib de la métropole. 
    Utilisez la carte interactive ci-dessous pour explorer les différentes stations.
    """)



    st.image('https://upload.wikimedia.org/wikipedia/commons/5/5b/V%C3%A9lib-M%C3%A9tropole-Logo.png', caption='Vélos Vélib dans Paris')
    
    # Affichage de la localisation de la station sur une carte
    #st.pydeck_chart(pdk.Deck(
        #map_style='mapbox://styles/mapbox/light-v9',
        #initial_view_state=pdk.ViewState(
            #latitude=station_info['lat'],
            #longitude=station_info['lon'],
            #zoom=15,
            #pitch=50,
        #),
        #layers=[
            #pdk.Layer(
                #'ScatterplotLayer',
                #data=pd.DataFrame([station_info]),  # Conversion de la ligne en DataFrame pour la compatibilité
                #get_position='[lon, lat]',
                #get_color='[200, 30, 0, 160]',
            # get_radius=100,
            #),
        #],
    #))

    #Création de la carte avec Plotly Express
    fig = px.scatter_mapbox(df,
                            lat="lat",
                            lon="lon",
                            hover_name="name",
                            hover_data={"lat":False, "lon":False, "vélos_disponibles":True, "places_disponibles":True},
                            zoom=12,
                            height=300,
                            mapbox_style="carto-positron"
    )
    fig.update_layout(
        width=1200,  # Largeur en pixels
        height=800  # Hauteur en pixels
    )
    st.plotly_chart(fig)

elif choose=="Choisis ta station":   
    st.title("Statut des Stations Vélib Métropole")
    st.markdown("""
    Cette application fournit un aperçu en temps réel de la disponibilité des vélos et des places dans les stations Vélib de la métropole. 
    Utilisez la carte interactive ci-dessous pour explorer les différentes stations.
    """)
    #Trier les noms des stations par ordre alphabétique pour le widget de sélection
    sorted_stations = sorted(df['name'].unique())


    # Widget de sélection pour choisir une station, maintenant avec les noms triés
    station_name  = st.selectbox("Choisissez une station:", sorted_stations)

    # Récupération des informations de la station sélectionnée
    station_info = df[df['name'] == station_name].iloc[0]

    # Affichage des informations de la station
    st.write(f"Vélos disponibles : {station_info['vélos_disponibles']}")
    st.write(f"Places disponibles : {station_info['places_disponibles']}")
    #Création de la carte avec Plotly Express
    fig = px.scatter_mapbox(pd.DataFrame([station_info]),
                            lat="lat",
                            lon="lon",
                            hover_name="name",
                            hover_data={"lat":False, "lon":False, "vélos_disponibles":True, "places_disponibles":True},
                            zoom=12,
                            height=300,
                            mapbox_style="carto-positron"
    )
    fig.update_layout(
        width=1200,  # Largeur en pixels
        height=800  # Hauteur en pixels
    )
    st.plotly_chart(fig)

# Run the app
#if __name__ == "__main__":
#    main()