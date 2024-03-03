import json
from mrjob.job import MRJob, MRStep
from mrjob.protocol import JSONProtocol

class MRMyJob(MRJob):

    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, _, line):
        object = json.loads(line)
        product_category = object['product_category']
        stars = int(object['stars'])
        language = object['language']
        product_id = object['product_id']
        review_length = len(object['review_body'])
        yield product_category, (stars, language, product_id, review_length)

    def product_category_reducer(self, key, values):
        
        # Variables
        stars_total = 0
        five_stars_reviews = 0
        products_list = []
        languages = {
            'es': 0,
            'en': 0,
            'fr': 0,
            'de': 0,
            'ja': 0,
            'zh': 0
        }

        # Output variables
        average_stars = 0
        most_popular_language = ''
        unique_products = 0
        total_reviews = 0
        minimum_chars = 2000

        # Reducing
        for value in values:
            
            # Get info from mapping
            stars = value[0]
            language = value[1]
            product_id = value[2]
            review_length = value[3]

            # Reduce operations
            if stars == 5:
                five_stars_reviews += 1

            stars_total += stars
            total_reviews += 1
            languages[language] += 1
            minimum_chars = min(review_length, minimum_chars)
            products_list.append(product_id)

        average_stars = stars_total / total_reviews
        unique_products = len(list(set(products_list)))
        most_popular_language = max(languages, key=languages.get)

        # Output
        yield key, (average_stars, most_popular_language, unique_products, total_reviews)
        yield 'global', (languages, minimum_chars, five_stars_reviews)

    def final_reducer(self, key, values):
        final_dict = {}

        # Global yield
        if key == 'global':
            
            total_five_stars = 0
            minimum_chars_ever = 2000
            languages_total = {
                'es': 0,
                'en': 0,
                'fr': 0,
                'de': 0,
                'ja': 0,
                'zh': 0
            }

            for value in values:
                languages = value[0]
                minimum_chars = value[1]
                five_stars_reviews = value[2]

                total_five_stars += five_stars_reviews
                minimum_chars_ever = min(minimum_chars_ever, minimum_chars)
                for language in languages_total.keys():
                    languages_total[language] += languages[language]

            global_dict = {
                'global': {
                    'Total number of reviews per language': languages_total,
                    'Minimum Number of characters for all the reviews': minimum_chars_ever,
                    'Total number of reviews with 5 stars': total_five_stars
                }
            }
            final_dict.update(global_dict)

        else:

            for value in values:
                average_stars = value[0]
                most_popular_language = value[1]
                unique_products = value[2]
                total_reviews = value[3]

            product_category = {
                key: {
                    'Average number of stars for the product category': average_stars,
                    'Language with the maximum number of reviews for this product category': most_popular_language,
                    'Total number of different products': unique_products,
                    'Total number of reviews per category': total_reviews
                }
            }

            final_dict.update(product_category)


        yield None, final_dict

    def steps(self):
        return[MRStep(mapper=self.mapper, reducer=self.product_category_reducer), MRStep(reducer=self.final_reducer)
        ]
    
if __name__ == "__main__":
    MRMyJob.run()