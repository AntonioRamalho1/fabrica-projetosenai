import plotly.express as px

def plot_line(df, x, y, title=None, labels=None):
    fig = px.line(df, x=x, y=y, title=title, labels=labels)
    fig.update_traces(line=dict(width=3), marker=dict(size=5), showlegend=False)
    fig.update_layout(margin=dict(l=20,r=20,t=40,b=20))
    return fig

def plot_bar(df, x, y, title=None, labels=None):
    fig = px.bar(df, x=x, y=y, title=title, labels=labels)
    fig.update_layout(margin=dict(l=20,r=20,t=40,b=20))
    return fig