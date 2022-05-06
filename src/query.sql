select r.name, year, max(score) from exam
    left join educational_institution ei on exam.educational_institution_id = ei.id
    left join place p on p.id = ei.place_id
    left join region r on r.id = p.region_id
    where subject_id = (select id from subject where code = 'Phys') group by r.name, year;