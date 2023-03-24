import pycarol


# name = "get_related_modules"
name = "get_question_tests"
carol = pycarol.Carol(dotenv_path=".env")
params = {'module': 'Pedidos de Vendas (MPD)', 'segment': 'Supply'}
query = pycarol.Query(carol, get_aggs=True, only_hits=True)
response = query.named(named_query=name, json_query=params).go().results
print(response)
print(len(response))
print(len(response[0]["hits"]))
