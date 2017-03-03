package pt.uminho.haslab.safecloudclient.shareclient;

public class ResultPlayerLoadBalancerImpl implements ResultPlayerLoadBalancer {

    //Default value is 0;
   private int targetPlayer;

    public synchronized int getResultPlayer() {
        return (targetPlayer++)%3;
    }
    
}
