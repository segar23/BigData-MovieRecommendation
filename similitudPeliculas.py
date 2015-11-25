# Calcular la similitud entre peliculas representando cada pelicula
# como un vector de ratings y calcular la similitud entre estos vectores

from mrjob.job import MRJob
from metrics import correlation
from metrics import cosine, regularized_correlation
from math import sqrt

try:
    from itertools import combinations
except ImportError:
    from metrics import combinations

PRIOR_COUNT = 10
PRIOR_CORRELATION = 0

class SemiColonValueProtocol(object):

    def write (self, key, values):
        return ';'.join(str(v) for v in values)

class SimilitudPeliculas (MRJob):

    OUTPUT_PROTOCOL = SemiColonValueProtocol

    def steps (self):
        return [
            self.mr(mapper=self.agrupar_por_rating_usuario,
                    reducer=self.contar_rating_usuario),
            self.mr(mapper=self.parejas_items,
                    reducer=self.calcular_similitud),
            self.mr(mapper=self.calcular_ranking,
                    reducer=self.top_items_similares)
        ]

    def agrupar_por_rating_usuario(self, key, line):
        """
        Crea una lista con los id de usuario, con el item que evaluan y el rating

         ID Item,Rating

        """
        user_id, item_id, rating = line.split('|')
        #rating = rating[:-1]
        yield user_id, (item_id, float(rating))

    def contar_rating_usuario(self, user_id, values):
        """
        Para cara usuario crea una linea con sus post:

         ID conteo_Items, suma_Items,(Item, Rating)
        """

        conteo_items = 0
        suma_items = 0
        final = []
        for item_id, rating in values:
            conteo_items += 1
            suma_items += rating
            final.append((item_id, rating))

        yield user_id, (conteo_items, suma_items, final)

    def parejas_items(self, user_id, values):
        """
        Aca se elimina el usuario de la clave
         Se emite una nueva clave como pares de peliculas, con
         su combinacion de ratings
        """

        conteo_items, suma_items, ratings = values
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), \
                   (item1[1], item2[1])

    def calcular_similitud(self, pair_key, lines):
        """
        Se suman los componentes de cada par con todos los usuarios que
         calificaron los items X y Y del par, luego se calcula la similitud
         de Pearson. Esta es normalizada entre [0,1] por el sort numerico
        """

        sum_xx, sum_xy, sum_yy, sum_x, sum_y, n = (0.0, 0.0, 0.0, 0.0, 0.0, 0)
        item_pair, co_ratings = pair_key, lines
        item_xname, item_yname = item_pair
        for item_x, item_y in lines:
            sum_xx += item_x * item_x
            sum_yy += item_y * item_y
            sum_xy += item_x * item_y
            sum_x += item_x
            sum_y += item_y
            n += 1

        corr_sim = correlation(n, sum_xy, sum_x,
                               sum_y, sum_xx, sum_yy)

        reg_corr_sim = regularized_correlation(n, sum_xy, sum_x, sum_y,
                                               sum_xx, sum_yy, PRIOR_COUNT, PRIOR_CORRELATION)

        cos_sim = cosine(sum_xy, sqrt(sum_xx), sqrt(sum_yy))

        jaccard_sim = 0.0

        yield (item_xname, item_yname), (corr_sim, cos_sim,
                                         reg_corr_sim, jaccard_sim, n)

    def calcular_ranking(self, item_keys, values):
        """
        Emite items con similitud en clave por ranking:

         X, similitud Y, n
        """

        corr_sim, cos_sim, reg_corr_sim, jaccard_sim, n = values
        item_x, item_y = item_keys
        if int(n) > 0:
            yield (item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim), \
                  (item_y, n)

    def top_items_similares(self, key_sim, similar_ns):
        """
        Por cada item emite los K items mas cercanos
        """

        item_x, corr_sim, cos_sim, reg_corr_sim, jaccard_sim = key_sim
        for item_y, n in similar_ns:
            yield None, (item_x, item_y, corr_sim, cos_sim, reg_corr_sim,
                         jaccard_sim, n)

if __name__ == '__main__':
    SimilitudPeliculas.run()
