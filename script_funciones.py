'''SCRIPT  DE FUNCIONES PARA EL MANEJO DE LOS DATASET METEOROLOGICOS PROPORCIONADOS POR: https://datos.madrid.es/'''

def wrangling_data_meteo(data):

    import pandas as pd
    import warnings
    warnings.filterwarnings('ignore') # ignore warnings
    
    ## CONVERSION NOMBRE COLUMNAS
    data.columns = [column.lower().replace(' ','_') for column in data.columns]
    
    ## MAPEO DE LAS MAGNITUDES 
    mag = {80: 'rad_ultravioleta',81:'vel_Viento',82:'dir_viento', 83: 'temperatura',
        86:'humedad_rel', 87:'presion_barometrica', 88: 'rad_solar',89:'precipitacion'}
    
    data['magnitud'] = data['magnitud'].map(mag)

    ## CONVERSION DE COLUMNAS DIAS A FECHA
    dias_maximos = {
        1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
    }

    dias = [f"d{str(i).zfill(2)}" for i in range(1, 32)]

    ## APLICACION DE MELT() 
    data_melted = data.melt(
        id_vars=["provincia", "municipio", "estacion", "magnitud", "punto_muestreo", "ano", "mes"],  ## columnas que se quedan fijas
        value_vars=dias,  ## columnas que van a ser filas
        var_name="dia",  ## nombre de la nueva columna
        value_name="value"  #nombre de la columna que almacenara los valores de cada dia
    )
    ## MANEJO DE LA COLUMNA FECHA
    data_melted['dia'] = data_melted['dia'].str[1:].astype("int64")
    data_melted = data_melted[data_melted['dia'] <= data_melted['mes'].map(dias_maximos)] ##filtramos para los meses que no tengan 31 dias
    data_melted["fecha"] = data_melted["dia"].astype(str) + '-' + data_melted["mes"].astype(str).str.zfill(2) + '-' + data_melted["ano"].astype(str).str.zfill(2)
    data_melted["fecha"] = pd.to_datetime(data_melted["fecha"], format='%d-%m-%Y', errors='coerce')
    data_melted["fecha"] = data_melted["fecha"].dt.strftime('%d/%m/%Y')

    data_melted.drop(columns=['ano','mes', 'dia'], inplace = True) ## dropeo de columnas

    ## PIVOTEAMOS LA TABLA
    ## Creamos un DF vacio con el nombre de las columnas que queremos
    pivot_df = pd.DataFrame(columns=['estacion','punto_muestreo','temperatura','precipitacion','vel_Viento',
                                     'presion_barometrica','dir_viento','humedad_rel','rad_solar'])
    ## itero por todas las estaciones utilizando .unique()
    for estacion in data_melted.estacion.unique():
        ##filtro por el id de estacion
        aux = data_melted[data_melted.estacion==estacion]
        ## se hace el pivot dejando como indice la fecha, y pasando como columna magnitud y valores el value
        aux_pivot = aux.pivot(index='fecha',columns='magnitud',values='value')
        ## creo una nueva columna estacion en aux pivot que almacene el iterador
        aux_pivot['estacion'] = estacion
        ## concateno el dataframe vacio y el nuevo
        pivot_df = pd.concat([pivot_df, aux_pivot])

    df_estaciones = pivot_df[pivot_df['estacion'].isin([24, 54, 56, 59])]
    
    ## lista de columnas a dropear
    list_columns_drop = ['punto_muestreo','rad_solar','rad_ultravioleta']

    for column in list_columns_drop:
        if column in df_estaciones:
            df_estaciones = df_estaciones.drop(columns = [column])
    
    df_estaciones.reset_index(inplace=True)
    df_estaciones.rename(columns={'index':'fecha'}, inplace=True)
    
    return df_estaciones




def wrangling_data_agentes(data):
    '''FUNCION MANEJO DATA DE AGENTES CONTAMINANTES'''

    import pandas as pd
    import warnings

    warnings.filterwarnings('ignore') # ignore warnings
    ## CONVERSION NOMBRE COLUMNAS
    data.columns = [column.lower().replace(' ','_') for column in data.columns]

    ## MAPEO DE LAS MAGNITUDES 
    mag_agentes = {1: 'dioxido_azufre',6:'monoxido_carbono',7:'monoxido_nitrogeno', 8: 'dioxido_nitrogeno',
        9:'pm_25', 10:'pm_10', 12: 'oxido_nitrogeno',14:'ozono',20:'tolueno',30:'benceno', 35:'etilbenceno',
        37:'metaxileno',38:'paraxileno',39:'ortoxileno',42:'hidrocarburos_totales',43:'metano',
        44:'hidrocarburos_no_metanicos', 431:'metaparaxileno'}

    data['magnitud'] = data['magnitud'].map(mag_agentes)

    ## CONVERSION DE COLUMNAS DIAS A FECHA
    dias_maximos = {
        1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
    }

    dias = [f"d{str(i).zfill(2)}" for i in range(1, 32)]

    ## APLICACION DE MELT() 
    data_melted = data.melt(
        id_vars=["provincia", "municipio", "estacion", "magnitud", "punto_muestreo", "ano", "mes"],  ## columnas que se quedan fijas
        value_vars=dias,  ## columnas que van a ser filas
        var_name="dia",  ## nombre de la nueva columna
        value_name="value"  #nombre de la columna que almacenara los valores de cada dia
    )
    ## MANEJO DE LA COLUMNA FECHA
    data_melted['dia'] = data_melted['dia'].str[1:].astype("int64")
    data_melted = data_melted[data_melted['dia'] <= data_melted['mes'].map(dias_maximos)] ##filtramos para los meses que no tengan 31 dias
    data_melted["fecha"] = data_melted["dia"].astype(str) + '-' + data_melted["mes"].astype(str).str.zfill(2) + '-' + data_melted["ano"].astype(str).str.zfill(2)
    data_melted["fecha"] = pd.to_datetime(data_melted["fecha"], format='%d-%m-%Y', errors='coerce')
    data_melted["fecha"] = data_melted["fecha"].dt.strftime('%d/%m/%Y')

    data_melted.drop(columns=['ano','mes', 'dia'], inplace = True) 

    ## PIVOTEAMOS LA TABLA
    ## Creamos un DF vacio con el nombre de las columnas que queremos
    pivot_df = pd.DataFrame(columns=['estacion','punto_muestreo','dioxido_azufre', 'monoxido_carbono', 'monoxido_nitrogeno',
                                    'dioxido_nitrogeno', 'oxido_nitrogeno', 'pm_25', 'pm_10', 'ozono','tolueno', 'benceno', 
                                    'etilbenceno', 'hidrocarburos_totales', 'metano', 'hidrocarburos_no_metanicos'])

    ## itero por todas las estaciones utilizando .unique()
    for estacion in data_melted.estacion.unique():
        ##filtro por el id de estacion
        aux = data_melted[data_melted.estacion==estacion]
        ## se hace el pivot dejando como indice la fecha, y pasando como columna magnitud y valores el value
        aux_pivot = aux.pivot(index='fecha',columns='magnitud',values='value')
        ## creo una nueva columna estacion en aux pivot que almacene el iterador
        aux_pivot['estacion'] = estacion
        ## concateno el dataframe vacio y el nuevo
        pivot_df = pd.concat([pivot_df, aux_pivot])

    df_estaciones_agentes = pivot_df[pivot_df['estacion'].isin([24, 54, 56, 59])]
    df_estaciones_agentes = df_estaciones_agentes.drop(columns =['punto_muestreo','dioxido_azufre','monoxido_carbono','tolueno', 'benceno','etilbenceno','hidrocarburos_totales','metano','hidrocarburos_no_metanicos'] )
    
    ## reseteo el indice, para no sea fecha y renombro

    df_estaciones_agentes.reset_index(inplace=True)
    df_estaciones_agentes.rename(columns={'index':'fecha'}, inplace=True)
    
    return df_estaciones_agentes