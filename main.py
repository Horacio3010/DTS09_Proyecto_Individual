from fastapi import FastAPI
import pandas as pd

app = FastAPI(title='Proyecto Individual', description='Data 09', version='1.0.1')

@app.get('/')
async def read_root():
    return {'API desarrollada para consultas de streaming '}

@app.on_event('startup')
async def startup():
    global dfcatalogo
    dfcatalogo = pd.read_csv(r'C:\Users\oomph\Documents\Henry Data Science\DTS09_Proyecto_Individual\df_catalogo_v2.csv')

#1 Película (sólo película, no serie, ni documentales, etc) con mayor duración según año, plataforma y tipo de duración.  La función debe llamarse get_max_duration(year, platform, duration_type) y debe devolver sólo el string del nombre de la película.

@app.get('/get_max_duration')
def get_max_duration(year: int, platform: str, duration_type: str) -> dict:
    # Filtrar el dataframe por tipo de contenido, año y plataforma
    filtered_df = dfcatalogo[(dfcatalogo['type'] == 'movie') & 
                            (dfcatalogo['release_year'] == year) & 
                            (dfcatalogo['plataforma'] == platform)]
    
    # Obtener la película con la duración máxima en el tipo especificado
    max_duration = filtered_df[filtered_df['duration_type'] == duration_type]['duration_int'].max()
    max_duration_movie = filtered_df[(filtered_df['duration_int'] == max_duration) &
                                     (filtered_df['duration_type'] == duration_type)]['title'].values
    
    # Verificar que el array de resultados no está vacío antes de intentar acceder a su primer elemento
    if len(max_duration_movie) > 0:
        return {'pelicula': max_duration_movie[0]}
    else:
        return {'pelicula': ''}
            
#2 Cantidad de películas (sólo películas, no series, ni documentales, etc) según plataforma, con un puntaje mayor a XX en determinado año.  La función debe llamarse get_score_count(platform, scored, year) y debe devolver un int, con el total de películas que cumplen lo solicitado.

@app.get('/get_score_count')
def get_score_count(platform: str, scored: float, year: int ):
    # Seleccionar  las filas donde el type es "movie" y la plataforma coinciden
    df_platform = dfcatalogo[(dfcatalogo['type'] == 'movie') & (dfcatalogo['plataforma'] == platform)]

    # Filtramos por "release_year" y "avg_rating"
    df_filtered = df_platform[(df_platform['release_year'] == year) & (df_platform['avg_rating'] >= scored)]

    # Retornamos un int con el total de peliculas que cumplen con las condiciones 
    return int(df_filtered.shape[0])

#3 Cantidad de películas (sólo películas, no series, ni documentales, etc) según plataforma. La función debe llamarse get_count_platform(platform) y debe devolver un int, con el número total de películas de esa plataforma. Las plataformas deben llamarse amazon, netflix, hulu, disney.

@app.get('/get_count_platform')
def get_count_platform(platform: str):
    # Seleccionar solo las películas de la plataforma especificada
    peliculas = dfcatalogo[(dfcatalogo['plataforma'] == platform) &
                   (dfcatalogo['type'] == 'movie')]
    # Obtener la cantidad de películas que cumplen los criterios
    cantidad = len(peliculas)
    return {
        'plataforma': platform,
        'peliculas': cantidad
    }

#4 Actor que más se repite según plataforma y año. La función debe llamarse get_actor(platform, year) y debe devolver sólo el string con el nombre del actor  que más se repite según la plataforma y el año dado.

@app.get('/get_actor')
def get_actor(platform:str, year:int ):
    # Filtramos el DataFrame según la plataforma y el año proporcionados
    df_filtered = dfcatalogo[(dfcatalogo['plataforma'] == platform) & (dfcatalogo['release_year'] == year)]

    # Separamos los nombres de los actores en una lista y eliminamos los valores nulos
    actor_list = df_filtered['cast'].dropna().str.split(',').explode().str.strip()

    # Contamos el número de veces que cada actor aparece en la lista
    actor_counts = actor_list.value_counts()

    # Devolvemos el nombre del actor que aparece con más frecuencia
    respuesta = actor_counts.idxmax()

    return {
        'plataforma': platform,
        'anio': year,
        'actor': respuesta,
        'apariciones': actor_counts[respuesta]
    }

#5 La cantidad de contenidos/productos (todo lo disponible en streaming) que se publicó por país y año.  La función debe llamarse prod_per_county(tipo,pais,anio) deberia devolver el tipo de contenido (pelicula,serie,documental) por pais  y año en un diccionario con las variables llamadas 'pais' (nombre del pais), 'anio' (año), 'pelicula' (tipo de contenido).

@app.get('/prod_per_county')
def prod_per_county(tipo: str, pais: str, anio: int):
    # Filtramos el DataFrame según el tipo de contenido, el país y el año
    df_filtered = dfcatalogo[(dfcatalogo['type'] == tipo) & (dfcatalogo['country'] == pais) & (dfcatalogo['release_year'] == anio)]

    # Contamos el número total de elementos del DataFrame filtrado
    total_peliculas = df_filtered.shape[0]

    return {'pais': pais, 
            'anio': anio, 
            'total de contenido': total_peliculas,
            'tipo_contenido': tipo}

#6 La cantidad total de contenidos/productos (todo lo disponible en streaming, series, documentales, peliculas, etc)  según el rating de audiencia dado (para que publico fue clasificada la pelicula). La función debe llamarse get_contents(rating)  y debe devolver el numero total de contenido con ese rating de audiencias.

@app.get('/get_contents')
def get_contents(rating:str):
    # Filtremos el dataframe dfcatalogo por el rating de audiencia dado
    filtros = dfcatalogo['rating'] == rating
    
    # Calculemos la cantidad total de contenidos/productos con ese rating
    cantidad = dfcatalogo.loc[filtros].shape[0]
    
    # Devolvemos la cantidad total de contenidos/productos con ese rating
    return cantidad

