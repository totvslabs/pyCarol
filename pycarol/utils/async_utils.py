def custom_exception_handler(loop, context):
    # first, handle with default handler
    loop.default_exception_handler(context)

    exception = context.get('exception')
    if isinstance(exception, ZeroDivisionError):
        print(context)
        loop.stop()