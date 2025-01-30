""" Includes the main function and provides example usages of your queries from queries db script.py."""

import pandas as pd


def execute_query(connection, query_func, *args, max_width=32):
    """
    Executes the given query function, prints its documentation,
    and displays the first results in a DataFrame, including column names.
    :param connection: connection to database.
    :param query_func: query to execute.
    :param max_width: maximum width of DataFrame columns.
    :param args: arguments to pass to query_func.
    """
    try:
        # Print function docstring (query documentation)
        if query_func.__doc__:
            docstring = query_func.__doc__.strip()
        else:
            docstring = "No documentation available."
        print(f"\nQuery Documentation for {query_func.__name__}:\n{docstring}\n")

        results, column_names = query_func(connection, *args)

        if results:
            df = pd.DataFrame(results, columns=column_names)

            # custom formatter for truncating and padding cells
            def custom_formatter(x):
                x = str(x)
                if len(x) > max_width:
                    return x[:max_width - 3] + '...'
                return x.ljust(max_width)

            formatters = {}
            for col in df.columns:
                formatters[col] = custom_formatter

            formatted_column_names = []
            for col in df.columns:
                formatted_column_names.append(custom_formatter(col))

            print(''.join(formatted_column_names))

            print(df.to_string(index=False, formatters=formatters, header=False))
        else:
            print(f"\nNo results found for {query_func.__name__}.")

    except Exception as e:
        print(f"Error executing {query_func.__name__}: {e}")
