from orchestrate.s2425.age_group import compute_age_group_winners
from tdfio.const import Gender

if __name__ == '__main__':
    male_ag = compute_age_group_winners(Gender.male)
    female_ag = compute_age_group_winners(Gender.female)

    male_ag.write_csv('orchestrate/s2425/age_group/male_age_group_winners.csv')
    female_ag.write_csv('orchestrate/s2425/age_group/female_age_group_winners.csv')
