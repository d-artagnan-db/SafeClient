package pt.uminho.haslab.safeclient.shareclient;

import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.util.List;

public class SharedCoprocessorRpcChannel extends CoprocessorRpcChannel {

    private List<CoprocessorRpcChannel> channels;
    private RpcController controller;
    static final Log LOG = LogFactory.getLog(SharedCoprocessorRpcChannel.class.getName());

    public SharedCoprocessorRpcChannel(List<CoprocessorRpcChannel> channels){

        this.channels = channels;
    }


    @InterfaceAudience.Private
    @Override
    public void callMethod(Descriptors.MethodDescriptor method, RpcController controller, Message request, Message responsePrototype, RpcCallback<Message> callback) {
        this.controller = controller;
        super.callMethod(method, controller, request, responsePrototype, callback);

    }

    @InterfaceAudience.Private
    @Override
    public Message callBlockingMethod(Descriptors.MethodDescriptor method, RpcController controller, Message request, Message responsePrototype) throws ServiceException {
        this.controller = controller;
        return super.callBlockingMethod(method, controller,request, responsePrototype);
    }

    @Override
    protected Message callExecService(Descriptors.MethodDescriptor methodDescriptor, Message message, Message message1) {
        Message msg = null;

        for(CoprocessorRpcChannel channel: channels){
            try {
                msg = channel.callBlockingMethod(methodDescriptor, controller, message, message1);
            } catch (ServiceException e) {
                LOG.error(e);
                throw new IllegalStateException(e);
            }
        }
        return msg;
    }
}
