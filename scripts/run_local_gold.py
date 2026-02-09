import sys
import os
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.gold.model_gold import GoldModeler
from scripts.gold.aggregate_gold import GoldAggregator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger('run_local_gold')

def main():
    try:
        logger.info('Starting local GOLD run')
        gm = GoldModeler()
        ga = GoldAggregator()

        logger.info('Running GoldModeler.run()')
        modeling_results = gm.run()
        logger.info(f'Modeling results: {modeling_results}')

        logger.info('Running GoldAggregator.run()')
        aggregation_results = ga.run()
        logger.info(f'Aggregation results: {aggregation_results}')

        logger.info('Local GOLD run completed')
        return 0
    except Exception as e:
        logger.exception(f'Local GOLD run failed: {e}')
        return 1

if __name__ == '__main__':
    sys.exit(main())
