# Databricks notebook source
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from datetime import datetime

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += principal  # Add annual investment
        future_value *= (1 + monthly_interest_rate)
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates

def plot_future_value(scenarios):
    fig = go.Figure()
    for scenario in scenarios:
        x_ticks = scenario['x_ticks']
        future_values = scenario['future_values']
        investment_type = scenario['investment_type']
        name = scenario['name']

        if investment_type == 'monthly':
            x_label = 'Months'
        else:
            x_label = 'Years'

        fig.add_trace(go.Scatter(x=x_ticks, y=future_values, name=name))

    fig.update_layout(title='Future Value of Investment', xaxis_title=x_label, yaxis_title='Future Value', plot_bgcolor='white')

    if investment_type == 'monthly':
        fig.update_layout(xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(x_ticks))),
            ticktext=x_ticks,
            automargin=True,  # Automatically adjust the margins to fit the tick labels
            dtick=3  # Increase the spacing between x-axis tick labels
        ))

    fig.update_yaxes(tickformat='₹,.0f')  # Format y-axis labels with the currency code and commas
    fig.show()

def calculate_and_plot_future_value(scenarios):
    for scenario in scenarios:
        principal = scenario['principal']
        interest_rate = scenario['interest_rate']
        periods = scenario['periods']
        investment_type = scenario['investment_type']
        inflation_rate = scenario['inflation_rate']
        start_year = scenario['start_year']
        start_month = scenario['start_month']

        future_values, x_ticks = calculate_future_value(principal, interest_rate, periods, investment_type,
                                                       inflation_rate, start_year, start_month)
        scenario['x_ticks'] = x_ticks
        scenario['future_values'] = future_values

    plot_future_value(scenarios)

scenarios = [
    {
        'name': 'Investment',
        'principal': 180000,
        'interest_rate': 7.1,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.0,
        'start_year': 2019,
        'start_month': 2
    },
    {
        'name': 'Inflation',
        'principal': 180000,
        'interest_rate': 0.0,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.02,
        'start_year': 2019,
        'start_month': 2
    }
]

calculate_and_plot_future_value(scenarios)


# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from datetime import datetime

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += principal  # Add annual investment
        future_value *= (1 + monthly_interest_rate)
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates

def plot_future_value(scenarios):
    fig = go.Figure()
    for scenario in scenarios:
        x_ticks = scenario['x_ticks']
        future_values = scenario['future_values']
        investment_type = scenario['investment_type']
        name = scenario['name']

        if investment_type == 'monthly':
            x_label = 'Months'
        else:
            x_label = 'Years'

        fig.add_trace(go.Scatter(x=x_ticks, y=future_values, name=name))

    fig.update_layout(title='Future Value of Investment', xaxis_title=x_label, yaxis_title='Future Value', plot_bgcolor='white')

    if investment_type == 'monthly':
        fig.update_layout(xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(x_ticks))),
            ticktext=x_ticks,
            automargin=True,  # Automatically adjust the margins to fit the tick labels
            dtick=3,  # Increase the spacing between x-axis tick labels
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10)  # Adjust the font size for better readability
        ))

    fig.update_yaxes(tickformat='₹,.0f')  # Format y-axis labels with the currency code and commas
    fig.show()

def calculate_and_plot_future_value(scenarios):
    for scenario in scenarios:
        principal = scenario['principal']
        interest_rate = scenario['interest_rate']
        periods = scenario['periods']
        investment_type = scenario['investment_type']
        inflation_rate = scenario['inflation_rate']
        start_year = scenario['start_year']
        start_month = scenario['start_month']

        future_values, x_ticks = calculate_future_value(principal, interest_rate, periods, investment_type,
                                                       inflation_rate, start_year, start_month)
        scenario['x_ticks'] = x_ticks
        scenario['future_values'] = future_values

    plot_future_value(scenarios)

scenarios = [
    {
        'name': 'Investment',
        'principal': 180000,
        'interest_rate': 7.1,
        'periods': 15,
        'investment_type': 'yearly',
        'inflation_rate': 0.0,
        'start_year': 2019,
        'start_month': 2
    }
]

calculate_and_plot_future_value(scenarios)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from datetime import datetime

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += principal  # Add annual investment
        future_value *= (1 + monthly_interest_rate)
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates

def plot_future_value(scenarios):
    fig = go.Figure()
    for scenario in scenarios:
        x_ticks = scenario['x_ticks']
        future_values = scenario['future_values']
        investment_type = scenario['investment_type']
        name = scenario['name']

        if investment_type == 'monthly':
            x_label = 'Months'
        else:
            x_label = 'Years'

        fig.add_trace(go.Scatter(x=x_ticks, y=future_values, name=name))

    fig.update_layout(title='Future Value of Investment', xaxis_title=x_label, yaxis_title='Future Value', plot_bgcolor='white')

    if investment_type == 'monthly':
        fig.update_layout(xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(x_ticks))),
            ticktext=x_ticks,
            automargin=True,  # Automatically adjust the margins to fit the tick labels
            dtick=3,  # Increase the spacing between x-axis tick labels
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10)  # Adjust the font size for better readability
        ))

    fig.update_yaxes(tickformat='₹,.0f')  # Format y-axis labels with the currency code and commas

    # Add gridlines
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')

    # Add legend outside the plot
    fig.update_layout(legend=dict(
        orientation='h',
        yanchor='bottom',
        y=-0.2,
        xanchor='center',
        x=0.5
    ))

    # Adjust the size of the plot
    fig.update_layout(width=800, height=500)

    fig.show()

def calculate_and_plot_future_value(scenarios):
    for scenario in scenarios:
        principal = scenario['principal']
        interest_rate = scenario['interest_rate']
        periods = scenario['periods']
        investment_type = scenario['investment_type']
        inflation_rate = scenario['inflation_rate']
        start_year = scenario['start_year']
        start_month = scenario['start_month']

        future_values, x_ticks = calculate_future_value(principal, interest_rate, periods, investment_type,
                                                       inflation_rate, start_year, start_month)
        scenario['x_ticks'] = x_ticks
        scenario['future_values'] = future_values

    plot_future_value(scenarios)

scenarios = [
    {
        'name': 'Investment',
        'principal': 180000,
        'interest_rate': 7.1,
        'periods': 15,
        'investment_type': 'yearly',
        'inflation_rate': 0.0,
        'start_year': 2019,
        'start_month': 2
    },
            {
        'name': 'Inflation',
        'principal': 180000,
        'interest_rate': 0.0,
        'periods': 15,
        'investment_type': 'yearly',
        'inflation_rate': 0.02,
        'start_year': 2019,
        'start_month': 2
    }
]

calculate_and_plot_future_value(scenarios)


# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += principal  # Add annual investment
        future_value *= (1 + monthly_interest_rate)
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates


# COMMAND ----------

import plotly.io as pio

def plot_future_value(scenarios):
    fig = go.Figure()
    for scenario in scenarios:
        x_ticks = scenario['x_ticks']
        future_values = scenario['future_values']
        investment_type = scenario['investment_type']
        name = scenario['name']

        if investment_type == 'monthly':
            x_label = 'Months'
        else:
            x_label = 'Years'

        fig.add_trace(go.Bar(x=x_ticks, y=future_values, name=name))

    fig.update_layout(title='Future Value of Investment', xaxis_title=x_label, yaxis_title='Future Value', plot_bgcolor='white')

    if investment_type == 'monthly':
        fig.update_layout(xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(x_ticks))),
            ticktext=x_ticks,
            automargin=True,  # Automatically adjust the margins to fit the tick labels
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10)  # Adjust the font size for better readability
        ))
    else:
        fig.update_xaxes(
            tickformat="%m/%Y",
            dtick="M12",  # Display x-axis tick labels at every 12 months interval
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10),  # Adjust the font size for better readability
            range=[-0.5, len(x_ticks) - 0.5]  # Add space before the first and after the last tick
        )

    fig.update_yaxes(tickformat='₹,.0f')  # Format y-axis labels with the currency code and commas

    # Add gridlines
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')

    # Add legend outside the plot
    fig.update_layout(legend=dict(
        orientation='h',
        yanchor='bottom',
        y=-0.2,
        xanchor='center',
        x=0.5
    ))

    # Adjust the size of the plot to the width of the screen
    fig.update_layout(width=pio.templates['plotly'].layout.width)

    fig.show()


# COMMAND ----------




def calculate_and_plot_future_value(scenarios):
    for scenario in scenarios:
        principal = scenario['principal']
        interest_rate = scenario['interest_rate']
        periods = scenario['periods']
        investment_type = scenario['investment_type']
        inflation_rate = scenario['inflation_rate']
        start_year = scenario['start_year']
        start_month = scenario['start_month']

        future_values, x_ticks = calculate_future_value(principal, interest_rate, periods, investment_type,
                                                       inflation_rate, start_year, start_month)
        scenario['x_ticks'] = x_ticks
        scenario['future_values'] = future_values

    plot_future_value(scenarios)

scenarios = [
    {
        'name': 'Investment',
        'principal': 180000,
        'interest_rate': 7.1,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.0,
        'start_year': 2019,
        'start_month': 2
    }
    ,
            {
        'name': 'Inflation',
        'principal': 180000,
        'interest_rate': 0.0,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.02,
        'start_year': 2019,
        'start_month': 2
    }
]

calculate_and_plot_future_value(scenarios)


# COMMAND ----------

import plotly.io as pio

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from datetime import datetime
import plotly.io as pio

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += principal  # Add annual investment
        future_value *= (1 + monthly_interest_rate)
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates

def calculate_future_value(principal, interest_rate, periods, investment_type, inflation_rate, start_year, start_month):
    if investment_type == 'monthly':
        periods = periods * 12  # convert years to months
    elif investment_type != 'yearly':
        raise ValueError("Invalid investment type. Please specify 'yearly' or 'monthly'.")

    future_value = principal
    monthly_inflation_rate = (1 + inflation_rate) ** (1/12) - 1
    monthly_interest_rate = (1 + interest_rate / 100) ** (1/12) - 1

    future_values = []
    for i in range(periods):
        future_value += future_value * monthly_interest_rate
        future_value /= (1 + monthly_inflation_rate)
        future_values.append(future_value)

    x_ticks_dates = [datetime(start_year, start_month, 1).strftime('%m/%Y')]
    for i in range(1, periods + 1):
        date = datetime(start_year + (start_month + i - 1) // 12, (start_month + i - 1) % 12 + 1, 1)
        x_ticks_dates.append(date.strftime('%m/%Y'))

    return future_values, x_ticks_dates

def plot_future_value(scenarios):
    fig = go.Figure()
    for scenario in scenarios:
        x_ticks = scenario['x_ticks']
        future_values = scenario['future_values']
        investment_type = scenario['investment_type']
        name = scenario['name']

        if investment_type == 'monthly':
            x_label = 'Months'
        else:
            x_label = 'Years'

        principal_amounts = np.diff(future_values, prepend=future_values[0])
        interest_amounts = np.diff(future_values)
        
        fig.add_trace(go.Bar(x=x_ticks, y=interest_amounts, name='Interest Benefit', marker=dict(color='green'), 
                             base=future_values[:-1]))
        fig.add_trace(go.Bar(x=x_ticks, y=principal_amounts, name='Principal Amount', marker=dict(color='lightgray')))

    fig.update_layout(title='Future Value of Investment', xaxis_title=x_label, yaxis_title='Future Value', plot_bgcolor='white')

    if investment_type == 'monthly':
        fig.update_layout(xaxis=dict(
            tickmode='array',
            tickvals=list(range(len(x_ticks))),
            ticktext=x_ticks,
            automargin=True,  # Automatically adjust the margins to fit the tick labels
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10)  # Adjust the font size for better readability
        ))
    else:
        fig.update_xaxes(
            tickformat="%m/%Y",
            dtick="M12",  # Display x-axis tick labels at every 12 months interval
            tickangle=90,  # Rotate the tick labels vertically
            tickfont=dict(size=10),  # Adjust the font size for better readability
            range=[-0.5, len(x_ticks) - 0.5]  # Add space before the first and after the last tick
        )

    fig.update_yaxes(tickformat='₹,.2f')  # Format y-axis labels with two decimal places

    # Add gridlines
    fig.update_xaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    fig.update_yaxes(showgrid=True, gridwidth=0.5, gridcolor='lightgray')
    
    # Add legend outside the plot
    fig.update_layout(legend=dict(
        orientation='h',
        yanchor='bottom',
        y=-0.2,
        xanchor='center',
        x=0.5
    ))

    # Adjust the size of the plot to the width of the screen
    fig.update_layout(width=pio.templates['plotly'].layout.width)

    fig.show()


def calculate_and_plot_future_value(scenarios):
    for scenario in scenarios:
        principal = scenario['principal']
        interest_rate = scenario['interest_rate']
        periods = scenario['periods']
        investment_type = scenario['investment_type']
        inflation_rate = scenario['inflation_rate']
        start_year = scenario['start_year']
        start_month = scenario['start_month']

        future_values, x_ticks = calculate_future_value(principal, interest_rate, periods, investment_type,
                                                       inflation_rate, start_year, start_month)
        scenario['x_ticks'] = x_ticks
        scenario['future_values'] = future_values

    plot_future_value(scenarios)

scenarios = [
    {
        'name': 'Investment',
        'principal': 180000,
        'interest_rate': 7.1,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.0,
        'start_year': 2019,
        'start_month': 2
    },
        {
        'name': 'Inflation',
        'principal': 180000,
        'interest_rate': 0.0,
        'periods': 15,
        'investment_type': 'monthly',
        'inflation_rate': 0.02,
        'start_year': 2019,
        'start_month': 2
    }
]

calculate_and_plot_future_value(scenarios)


# COMMAND ----------


