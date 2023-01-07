-- DROP TABLE IF EXISTS studiescopy;

-- CREATE TABLE studiescopy AS SELECT * FROM studies; 
-- DELETE FROM studiescopy;
-- SELECT * FROM studiescopy;
-- 
DO $$
 DECLARE
     study_prefix   character(3);
     researcher character(255);
     study_program character(255);
     start_year int;
     end_year int;
     start_year_string character(2);
     end_year_string character(2);

 BEGIN
     study_prefix := 'PAL';
     researcher := 'Dr. Kristen Gorman';
     study_program := 'Palmer Station Long Term Ecological Research Program';

     FOR counter IN 7..9
         LOOP
            start_year := counter;
            end_year := counter + 1;
            IF start_year < 10 THEN
                start_year_string := '0' || start_year;
			ELSE
			    start_year_string := start_year || '';
            END IF;
            IF end_year < 10 THEN
                end_year_string := '0' || end_year; 
			ELSE
			    end_year_string := end_year || '';
            END IF;
            
            INSERT INTO studies(study_number, study_author, study_program)
             VALUES (study_prefix || start_year_string || end_year_string, researcher, study_program);
         END LOOP;
 END;
 $$ LANGUAGE plpgsql;

-- SELECT * FROM studiescopy;
