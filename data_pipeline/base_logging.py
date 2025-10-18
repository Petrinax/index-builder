import logging
from pathlib import  Path
# Configure the base logger

# class Logger(logging.Logger):
#     """Base logger configuration class"""
#
#
#     def __new__(cls, name: str) -> logging.Logger:
#         """Get a configured logger instance"""
#         logger = logging.getLogger(name)
#         logger.setLevel(logging.INFO)
#
#         # Create console handler
#         ch = logging.StreamHandler()
#         ch.setLevel(logging.INFO)
#
#         # Create file handler
#         fh = logging.FileHandler(f'{name}.log')
#         fh.setLevel(logging.INFO)
#
#         # Create formatter and add it to the handlers
#         formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#         ch.setFormatter(formatter)
#         fh.setFormatter(formatter)
#
#         # Add the handlers to the logger
#         logger.addHandler(ch)
#         logger.addHandler(fh)
#
#         return logger


# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.StreamHandler()
#     ]
# )

# def get_logger(name: str) -> logging.Logger:
#     """Get a configured logger instance"""
#
#     logger = logging.getLogger(name)
#     logger.addHandler(
#         logging.FileHandler(f'{name}.log')
#     )
#     return logger


class Logger(logging.Logger):
    """Base logger configuration class"""

    def __new__(cls, name: str) -> logging.Logger:
        """Get a configured logger instance"""

        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler( logs_dir / f'{name}.log')
            ]
        )
        logger = logging.getLogger(name)
        return logger

