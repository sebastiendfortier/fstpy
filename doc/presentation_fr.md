---
theme : "night"
highlightTheme: "monokai"
zoom: "75"
title: "Fstpy fr"
---

## Présentation Fstpy
Sébastien Fortier 2021

---

### Fstpy c'est Pandas!

---

### Définitions
- FST -> fichiers standards
- sorties de modèles $CMCGRIDF 

---

### Variables dans fichier FST
![image](cube.jpg)

---

### Origine
- Besoin d'une interface python pour Spooki
- Besoin d'une nouvelle structure de mémoire pour les métadonnées de fichiers FST

---

### C'est quoi?
- Interface de haut niveau ->  rpnpy de rpn
- Produit des Dataframes
- Favorise le découplage, la modularisation et la collaboration
- Lecture, décodage d'attributs et écriture

---

### Philosophie
- Façon de travailler "pythonic" avec des fichiers FST 
- Pas besoin de connaître tous les mécanismes de rmnlib
- Beaucoup de gens viennent ici avec des connaissances sur numpy, pandas et xarray
- Courbe d'apprentissage moins raide.

---

### API utilisés
- pandas
- numpy
- dask
- xarray


---

### Pandas
- Bons pour organiser l'information.
- Intégrer de nouveaux changements de modèle et de nouveaux types de données
- Exporter nos résultats plus facilement vers différents types de formats.
- <https://pandas.pydata.org/>

---

### numpy
- Bibliothèque pour la prise en charge de grande matrices multidimensionnelles
- Vaste collection de fonctions mathématiques de haut niveau pour opérer sur ces matrices
- <https://numpy.org/>
---

### Dask
- Bibliothèque open source pour le calcul parallèle écrite en Python
- Il est développé en coordination avec d'autres projets communautaires comme NumPy, pandas et scikit-learn.
- <https://dask.org/>

---

### Xarray
- Organisation analogue au format netCDF
- Analyse des données regroupées et indexées. 
- Données météorologiques n-dimensionnelles. 
- Fonctions de graphisme intégrées.
- Indexation simplifié grace aux dimensions nommées
- <http://xarray.pydata.org/en/stable/>

---

#### Les fichiers Standards
![image](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcS8HAeOTkPIH4k7xO_7dlM8Ks9ecoEqlsr-zQ&usqp=CAU)

---

### A quoi ressemble un fichier FST
- Tableau avec un enregistrement par ligne

```bash
0 [sbf000@eccc3-ppp4 ~/data] $ voir -iment source_data_5005.std

       NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s)           IP1       IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4

    0- HU   P  R1_V710_N        1108    1082     1 20200714 120000      95529009         6         0      300       72  f 16  Z 33792 77761     1     0
    1- HU   P  R1_V710_N        1108    1082     1 20200714 120000      97351772         6         0      300       72  f 16  Z 33792 77761     1     0
    2- GZ   P  R1_V710_N        1108    1082     1 20200714 120000      95364364         6         0      300       72  f 16  Z 33792 77761     1     0
    3- GZ   P  R1_V710_N        1108    1082     1 20200714 120000      95357866         6         0      300       72  f 16  Z 33792 77761     1     0
    4- TT   P  R1_V710_N        1108    1082     1 20200714 120000      95178882         6         0      300       72  f 16  Z 33792 77761     1     0

```

---

#### Fstpy
![image](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRef8630r2P860i3ZQYu1xkH5mPMj3l7xuAAw&usqp=CAU)

---

### Avec un Dataframe
```python
 0 [sbf000@eccc3-ppp4 ~/data] $ python
>>> import fstpy.all as fstpy
>>> df = fstpy.StandardFileReader('source_data_5005.std').to_pandas()
>>> df
     nomvar typvar     etiket    ni    nj  nk      dateo       ip1    ip2  ip3  deet  npas  datyp  nbits grtyp    ig1    ig2    ig3    ig4
0        HU      P  R1_V710_N  1108  1082   1  442998800  95529009      6    0   300    72    134     16     Z  33792  77761      1      0
1        HU      P  R1_V710_N  1108  1082   1  442998800  97351772      6    0   300    72    134     16     Z  33792  77761      1      0
2        GZ      P  R1_V710_N  1108  1082   1  442998800  95364364      6    0   300    72    134     16     Z  33792  77761      1      0
3        GZ      P  R1_V710_N  1108  1082   1  442998800  95357866      6    0   300    72    134     16     Z  33792  77761      1      0
4        TT      P  R1_V710_N  1108  1082   1  442998800  95178882      6    0   300    72    134     16     Z  33792  77761      1      0
...     ...    ...        ...   ...   ...  ..        ...       ...    ...  ...   ...   ...    ...    ...   ...    ...    ...    ...    ...
1869     HF      P  R1_V710_N  1104  1078   1  442998800  60168832      6    0   300    72    134     12     Z  35132  56748      1      0
1870   FATB      P  R1_V710_N  1104  1078   1  442998800  60368832      6    0   300    72    134     12     Z  35132  56748      1      0
1871     >>      X  R1_V710_N  1104     1   1  442998800     35132  56748    1     0     0      5     32     E   1470    560  54400  46560
1872     ^^      X  R1_V710_N     1  1078   1  442998800     35132  56748    1     0     0      5     32     E   1470    560  54400  46560
1873     !!      X  R1_V710_N     3   175   1          0     35132  56748    0     0     0      5     64     X   5005      0    300   1500

[1874 rows x 19 columns]


```
Étrangement similaire!

---

### Module du vent
```python
# lire le fichier
df = fstpy.StandardFileReader('source_data_5005.fst').to_pandas()
# sélectionner tous les UU
uu_df = df.loc[df.nomvar=='UU']
# sélectionner tous les UU
vv_df = df.loc[df.nomvar=='VV']
# créer un contenant pour la réponse
uv_df = copy.deepcopy(uu_df)
for idx in uu_df.index:
     uu = uu_df.at[idx,'d']
     vv = vv_df.at[idx,'d']
     uv = np.sqrt(uu**2+vv**2)
     uv_df.at[idx,'d'] = uv

fstpy.StandardFileWriter('resultat.fst',uv_df).to_fst()
```

---

### version 2
```python
# sélectionner tous les UU
uu_df = df.loc[df.nomvar=='UU']
# sélectionner tous les UU
vv_df = df.loc[df.nomvar=='VV']

uu = np.stack(uu_df.d)
vv = np.stack(vv_df.d)
uv = np.sqrt(uu**2+vv**2) # or np.hypot(uu,vv)
uv_df['d'] = uv

```

---

### version 3 xarray
```python
# sélectionner tous les UU
uu_df = df.loc[df.nomvar=='UU']
# sélectionner tous les UU
vv_df = df.loc[df.nomvar=='VV']

uu_da = xr.DataArray(np.stack(uu_df.d))
vv_da = xr.DataArray(np.stack(vv_df.d))
ds = xr.Dataset({'UU':uu_da,'VV':vv_da})
uv_da = np.hypot(ds.UU,ds.VV)

```

---

### Notes
- Documention sur les dataframe -> Google
- Aide -> Google/Stackoverflow
- Dataframe  -> netCDF avec fstd2nc direct avec Buffer.from_fstpy(df).to_netcdf("some_met_data.nc")
- Dataframe  -> Xarray avec fstd2nc direct avec Buffer.from_fstpy(df).to_xarray()

---

### Structure
- Documentation Sphinx
- Tests unitaires doctest
- Tests de regression pytest
- Packaging SSM

---

### Liens
- Documentation Fstpy <http://web.science.gc.ca/~spst900/fstpy/master/>
- Gitlab <https://gitlab.science.gc.ca/CMDS/fstpy>
- Documentation fstd2nc [Réseau EC] <https://wiki.cmc.ec.gc.ca/wiki/Fstd2nc>
- Documentation rpnpy [Réseau EC] <https://wiki.cmc.ec.gc.ca/wiki/Rpnpy>
- Outil Maestro dask cluster <https://gitlab.science.gc.ca/mde000/maestro-dask-cluster>

---

### Questions?


