from scripts.etl.extract_splitwise import exract_splitwise

exract_splitwise(
    user="Jakub", #Jakub or Lucja
    group_id=82641053
)
[(f.getName(), f.getId()) for f in sw.getGroups() if month in f.getName() and year in f.getName()]