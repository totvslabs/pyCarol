Online app: Building a simple GUI with Streamlit
================================================

On the previous chapter we've seen how to create an online app to
respond to POST and GET requests, through a REST architecture and JSON
communication pattern. On this chapter we will see how make a simple web
interface to make the user interaction more pleasent.

This online app is powered by the `streamlit <https://streamlit.io/>`__
library, which is a framework for easily create web apps based on data.
With Streamlit we will build a form interface that, once submited, the
form data is packed, evaluated by the model and then the result (the
house price prediction) is presented to the user.

Online app api files structure
------------------------------

For this example we will use the files below. The ``manifest.json`` will
follow the same configuration as the previous Online App, the
``requirements.txt`` will also be very similar, but with less
dependencies and adding ``streamlit`` lib on it.

::

    app.py
    Dockerfile
    manifest.json
    requirements.txt

The ``app.py`` will be the Docker image entrypoint, it is where we
create the web app using streamlit. The ``Dockerfile`` is close to the
one generated for the Online API, the only difference is the ``CMD``
command, which now launches our App with the ``streamlit run`` command.

::

    ...
    EXPOSE 5000
    CMD streamlit run --server.port 5000 app.py

This forementioned structure covers much of the cases that you may need.
Therefore, we usually only add our logic to ``app.py``, and packages to
``requirements.txt``. Keeping the ``Dockerfile`` as it is.

Code: Streamlit basics
----------------------

Streamlit provides a very simple way to create graphical interfaces: you
just need to instantiate classes for the desired components and
configure its parameters. Once the parameters are in place, every user
interaction will triger the whole script to be reexecuted.

The code below simply adds a header to our web page and provides a brief
description to the user.

::

    # Add title on the page.
    st.title("Boston House Prices")

    # Add a text in our page.
    st.write("Here, we can help you to find at what price it's recommended that you sell home.")

Next we add a form so that the user is able to fill all attributes for
the house he wants to evaluate. Notice that first we create a form, then
we add all the input fields and finally we add a submit button. A
``form`` is a particular component on streamlit that allows to freeze
the execution until the submit button is trigered, only then code
following will be executed.

::

    # Creates a form and all the necessary fields to be completed by user.
    form = st.form(key='my_form')

    crim = form.number_input('Per capita crime rate by town', format='%f', min_value=0) 
    zn = form.number_input('Proportion of residential land zoned for lots over 25,000 sq.ft.', format='%f', min_value=0.0)
    indus = form.number_input('Proportion of non-retail business acres per town.', format='%f', min_value=0.0)
    ...
    b = form.number_input('1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town.', format='%f', min_value=0.0)
    lstat = form.number_input(r'% lower status of the population', format='%f', min_value=0.0) 

    submit_button = form.form_submit_button(label='Predict house price')

Finally, after submitted, the user receives the model prediction.

::

    # When a user clicks on the submit button we predict and present in the screen the price of the house based on the information
    # that they have inputed in the interface.
    if submit_button:
        price = model_predict(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat)
        st.write(f'Predicted selling price for home: ${price}')

Deploying our app in Carol
--------------------------

To deploy your app follow the same steps described on the previous chapter.
