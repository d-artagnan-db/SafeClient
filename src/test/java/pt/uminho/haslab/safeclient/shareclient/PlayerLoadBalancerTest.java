package pt.uminho.haslab.safeclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
public class PlayerLoadBalancerTest {

	static final Log LOG = LogFactory.getLog(PlayerLoadBalancerTest.class
			.getName());

	@Test
	public void test() {

		ResultPlayerLoadBalancerImpl lb = new ResultPlayerLoadBalancerImpl();

		Map<Integer, Integer> counter = new HashMap<Integer, Integer>();

		for (int i = 0; i < 90; i++) {
			int target = lb.getResultPlayer();

			boolean contains = counter.containsKey(target);
			int total = 0;
			if (contains) {
				total = counter.get(target);
			}
			counter.put(target, ++total);

		}

		LOG.debug("Map size is " + counter.size());
		LOG.debug("Counter player 0 " + counter.get(0));
		LOG.debug("Counter player 1 " + counter.get(1));
		LOG.debug("Counter player 2 " + counter.get(2));

		assertEquals(3, counter.size());
		assertEquals(counter.get(0), counter.get(1));
		assertEquals(counter.get(1), counter.get(2));

	}
}
