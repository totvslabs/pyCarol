from myapp import MyApp
from pycarol.carol import *
from pycarol.auth.PwdAuth import *


if __name__ == '__main__':
    carol = Carol('rui', 'testApp', auth=PwdAuth('william.monti@totvs.com', 'totvs123'))
    app = MyApp(carol)
    if app.run('predict'):
        print("Success!")
    else:
        print("Failed!")

    validation = app.validate_data()
    print("Validation results")
    for val in validation:
        print(val)
