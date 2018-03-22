import pandas as pd

def funcWriteExcel(df, excel_writer, sheet_name='Sheet1'):
    if type(excel_writer) == str:
        excel_writer = pd.ExcelWriter(excel_writer)
        boolStrInput = True
    else:
        boolStrInput = False

    df.to_excel(excel_writer, sheet_name=sheet_name, startrow=1, float_format='%0.4f', header=None)
    workbook = excel_writer.book
    format = workbook.add_format()
    format.set_text_wrap()
    format.set_align('center')
    format.set_align('vcenter')
    format.set_bold()
    format.set_border()
    
    worksheet = workbook.get_worksheet_by_name(sheet_name)
    NDimensionIndex = len(df.index.names)
    for nColumn, strColumn in enumerate(df.columns):
        worksheet.write(0, nColumn+NDimensionIndex, strColumn, format)
    worksheet.freeze_panes(1, NDimensionIndex)

    if boolStrInput:
        excel_writer.close()

    return


