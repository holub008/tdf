from acquire.s2526.ashwabay import get_results
from orchestrate.s2526 import Event2526
from score.event import score_and_save_event

if __name__ == "__main__":
    main_results = get_results(True)
    nonmain_results = get_results(False)
    score_and_save_event(Event2526.ashwabay, main_results, nonmain_results)