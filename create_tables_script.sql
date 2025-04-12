create table if not exists encounters as select *
from read_csv_auto('~/projects/analyst-take-home-task/datasets/encounters.csv',
SAMPLE_SIZE = -1);
    
 create table if not exists procedures as select *
    from read_csv_auto('~/projects/analyst-take-home-task/datasets/procedures.csv', 
    SAMPLE_SIZE = -1);
 
create table if not exists patients as select *
    from read_csv_auto('~/projects/analyst-take-home-task/datasets/patients.csv', 
    SAMPLE_SIZE = -1);

create table if not exists medications as select *
    from read_csv_auto('~/projects/analyst-take-home-task/datasets/medications.csv', 
    SAMPLE_SIZE = -1);

create table if not exists allergies as select *
    from read_csv_auto('~/projects/analyst-take-home-task/datasets/allergies.csv', 
    SAMPLE_SIZE = -1);
    
    
