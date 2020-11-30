from db import DBProvider

def test_no_match_id_duplicates():
    db = DBProvider()
    request = """
SELECT match_id, COUNT(*) FROM (
    SELECT match_id FROM matches
    UNION SELECT match_id FROM current_matches
    UNION SELECT match_id FROM future_matches
) all_matches
GROUP BY match_id HAVING COUNT(*) > 1"""
    assert 0 == len(db.select(request))

def test_no_preprocessed_team_duplicates():
    db = DBProvider()
    assert 0 == len(db.select("SELECT team_home, COUNT(DISTINCT country) FROM preprocessed_matches GROUP BY team_home HAVING COUNT(DISTINCT country) > 1"))
    assert 0 == len(db.select("SELECT team_away, COUNT(DISTINCT country) FROM preprocessed_matches GROUP BY team_away HAVING COUNT(DISTINCT country) > 1"))
