import logging
import functools
import datetime

# Configure logging (do this once)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')


def slf4j(cls):
    """Class decorator to simulate Lombok's @Slf4j."""

    # Get a logger instance for the class
    logger = logging.getLogger(cls.__name__)

    def log_method(log_level, message=""):
        """Decorator for logging method calls."""

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                '''
                log_message = f"{timestamp} - {cls.__name__}.{func.__name__}"
                if message:
                    log_message += f" - {message}"

                log_function = getattr(logger, log_level.lower())  # get the correct log function

                log_function(
                    f"{log_message} - Calling with args: {args[1:] if args and hasattr(args[0], '__class__') else args}, kwargs: {kwargs}")
                '''
                class_name = ""
                if args and hasattr(args[0], '__class__'):
                    class_name = args[0].__class__.__name__ + "."

                full_log_message = f"{timestamp} - {class_name}{func.__name__}"
                if message:
                    full_log_message += f" - {message}"

                log_function = getattr(logger, log_level.lower())
                log_function(f"{full_log_message} - Calling with args: {args[1:] if args and hasattr(args[0], '__class__') else args}, kwargs: {kwargs}")


                try:
                    result = func(*args, **kwargs)
                    log_function(f"{full_log_message} - Returned: {result}")
                    return result
                except Exception as e:
                    logger.exception(f"{full_log_message} - Raised an exception: {e}")
                    raise

            return wrapper

        return decorator

    # Add logging methods to the class
    cls.log = logger  # add logger to the class
    '''
    cls.info = log_method("INFO")
    cls.debug = log_method("DEBUG")
    cls.warn = log_method("WARNING")
    cls.error = log_method("ERROR")
    '''
    cls.info = lambda message="": log_method("INFO", message) #fix is here
    cls.debug = lambda message="": log_method("DEBUG", message) #fix is here
    cls.warn = lambda message="": log_method("WARNING", message) #fix is here
    cls.error = lambda message="": log_method("ERROR", message) #fix is here

    return cls
