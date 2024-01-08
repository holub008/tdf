import polars as pl


def compute_age_advantage(rr):
    return rr.with_columns(
        pl.when(pl.col('age') <= 40)
        .then(0.0)
        .otherwise(pl.col('age').sub(40.0).truediv(2.0))
        .alias('age_advantage')
    )


def compute_event_points_with_age_advantage(gender_raw_results: pl.DataFrame) -> pl.DataFrame:
    max_place = gender_raw_results.select(pl.col('gender_place').max().alias('m')).item(0, 'm')
    rrp = gender_raw_results.with_columns(
        pl.lit(1).sub((pl.col('gender_place').sub(1)).truediv(max_place)).mul(100).alias('placement_points')
    )
    rrp_floored = rrp.with_columns(
        pl.when(pl.col('placement_points') >= 20.0)
        .then(pl.col('placement_points'))
        .otherwise(20.0)
        .alias('floored_placement_points')
    )

    waa = compute_age_advantage(rrp_floored)
    # no event incentives for the first event!
    fpp = waa.with_columns(
        pl.col('floored_placement_points').add(pl.col('age_advantage')).alias('total_event_points')
    )

    return fpp.with_columns(
        pl.when(pl.col('total_event_points') > 100.0).then(100.0)
        .otherwise(pl.col('total_event_points'))
        .alias('age_advantage_event_points')
    )


def attach_event_incentives(aggregate_results: pl.DataFrame) -> pl.DataFrame:
    """
    returned df has columns `racer_id` and `points`
    will provide an inner join on matched results (with 0 points for non-eligibles)
    """
    event_incentives = []
    for ar in aggregate_results.iter_rows(named=True):
        ei = 0
        if ar['n_events'] >= 3:
            ei += 15
        elif ar['n_events'] >= 6:
            ei += 20
        elif ar['n_events'] >= 9:
            ei += 25

        event_incentives.append(ei)

    aggregate_results = aggregate_results.with_columns(
        pl.Series(name='event_incentive_points', values=event_incentives)
    )
    return aggregate_results


def compute_total_individual_points(
        event_results: list,
        events: list
) -> pl.DataFrame:
    if not len(event_results) == len(events):
        raise ValueError('events and results must match in length')

    if len(event_results) < 2:
        raise ValueError('must supply 2 or more ')

    aggregate_results = event_results[0]
    aggregate_results = aggregate_results\
        .clone()\
        .rename({
            'age_advantage_event_points': f'{events[0].to_string()}_points',
        })\
        .select('first_name', 'last_name', f'{events[0].to_string()}_points')

    for _ix, other in enumerate(event_results[1:]):
        ix = _ix + 1
        event = events[ix]
        oc = other.select('first_name', 'last_name', 'age_advantage_event_points')\
            .rename({'age_advantage_event_points': f'{event.to_string()}_points'})
        # we can expect the validation to eventually raise a problem
        joined = aggregate_results.join(oc, on=['first_name', 'last_name'], how='outer')

        # validate that we didn't get any multi-joins
        if not joined.n_unique(subset=['first_name', 'last_name']) == joined.shape[0]:
            raise ValueError('Data contained a many-1 join on names')

        aggregate_results = joined

    total_event_points = []
    total_n_events = []
    for ar in aggregate_results.iter_rows(named=True):
        n_events = 0
        all_event_points = 0
        for e in events:
            event_points = ar[f'{e.to_string()}_points']
            # TODO
            if event_points is not None:
                n_events += 1
                all_event_points += event_points

        total_event_points.append(all_event_points)
        total_n_events.append(n_events)

    aggregate_results = aggregate_results\
        .with_columns(
            pl.Series(name='total_event_points', values=total_event_points),
            pl.Series(name='n_events', values=total_n_events)
        )

    ar_with_ei = attach_event_incentives(aggregate_results)
    ar_with_ei = ar_with_ei.with_columns(
        pl.col('total_event_points').add(pl.col('event_incentive_points')).alias('total_points')
    )
    return ar_with_ei


def compute_team_points():
    pass
