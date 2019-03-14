import dash_html_components as html
import dash_core_components as dcc
import dash_table


def string_item(str,str_type='', key=None,key_type='primary'):
    if key is not None:
        return html.Div(children=[ html.Div(children=key, className=f'badge badge-{key_type}'),
            html.Div(children=str,className=f'badge ')])
    else:
        return  html.Div(children=str)

def text_item(str,key=None,key_type='primary'):
    if key is not None:
        return html.Div(children=[ html.Div(children=key, className=f'badge badge-{key_type}'),
            dcc.Textarea(value=str, className='form-control',rows=20)])
    else:
        return  dcc.Textarea(value=str,className='form-control',rows=20)

def cmd_item(cmd,args=None):
    if args is not None:
        return html.Div(children=[ html.Div(children=f"plugins_manager:{cmd}", className=f'badge badge-primary'),
            html.Div(children=f"args:{args}",className=f'badge badge-info')])
    else:
        return   html.Div(children=f"cmd:{cmd}", className=f'badge badge-primary')


def dataframe_item(df):
    res = dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict("rows"),
    )


    # head_cols =  [html.Th(col, className='col text-center') for col in df.columns.values.tolist()]
    # head = html.Tr(children=  head_cols, className='row')
    #
    # data = [
    #     html.Tr(children=[ html.Td(value, className='col') for value in r ], className='row')
    #     for r in df.itertuples(index=False)]
    # res = [
    #     html.Table(children=[html.Thead(children=head), html.Tbody(children=data)], className='table'),
    # ]
    return res


def text_commit_item(text):

    res = [
        dcc.Textarea(id='commit-text-text', value=text,placeholder='what are you looking for ?',
                  className='form-control',rows=20),
        html.Button('modify', id='commit-text-btn', className='btn btn-primary'),
        html.Div(id='text_commit_result', className='row')
    ]
    return res

