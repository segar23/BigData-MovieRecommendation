#-*-coding: utf-8 -*-
# Métodos para convertir los output y enviarlos hacia MongoDB con la
# info que se requiera para el recomendador de peliculas

from pymongo import MongoClient


def converter(input):
    file = open(input)
    client = MongoClient()
    db = client.movieMatrix
    coll = db.movieCorrelation

    for line in file:
        movie_a, movie_b, correlation, cosine, reg_correlation, \
        jaccard, n = line.split(';')

        if float(correlation) > 0.9:
            result = coll.insert_one(
                {
                    "Movie A": movie_a,
                    "Movie B": movie_b,
                    "Correlation": float(correlation)
                }
            )

    file.close()


converter('ouput.csv')
