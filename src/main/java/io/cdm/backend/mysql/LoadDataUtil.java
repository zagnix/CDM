package io.cdm.backend.mysql;

import java.io.*;
import java.util.List;

import io.cdm.CDMServer;
import io.cdm.backend.BackendConnection;
import io.cdm.net.BackendIOConnection;
import io.cdm.net.mysql.BinaryPacket;
import io.cdm.route.RouteResultsetNode;
import io.cdm.sqlengine.mpp.LoadData;

/**
 * Created by nange on 2015/3/31.
 */
public class LoadDataUtil
{
    public static void requestFileDataResponse(byte[] data, BackendConnection conn)
    {

        byte packId= data[3];
        BackendIOConnection backendIOConnection = (BackendIOConnection) conn;
        RouteResultsetNode rrn= (RouteResultsetNode) conn.getAttachment();
        LoadData loadData= rrn.getLoadData();
        List<String> loadDataData = loadData.getData();
        try
        {
            if(loadDataData !=null&&loadDataData.size()>0)
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (int i = 0, loadDataDataSize = loadDataData.size(); i < loadDataDataSize; i++)
                {
                    String line = loadDataData.get(i);


                    String s =(i==loadDataDataSize-1)?line: line + loadData.getLineTerminatedBy();
                    byte[] bytes = s.getBytes(loadData.getCharset());
                    bos.write(bytes);


                }

                packId=   writeToBackConnection(packId,new ByteArrayInputStream(bos.toByteArray()), backendIOConnection);

            }   else
            {
                //从文件读取
                packId=   writeToBackConnection(packId,new BufferedInputStream(new FileInputStream(loadData.getFileName())), backendIOConnection);

            }
        }catch (IOException e)
        {

            throw new RuntimeException(e);
        }  finally
        {
            //结束必须发空包
            byte[] empty = new byte[] { 0, 0, 0,3 };
            empty[3]=++packId;
            backendIOConnection.write(empty);
        }




    }

    public static byte writeToBackConnection(byte packID, InputStream inputStream, BackendIOConnection backendIOConnection) throws IOException
    {
        try
        {
            int packSize = CDMServer.getInstance().getConfig().getSystem().getBufferPoolChunkSize() - 5;
            // int packSize = backendIOConnection.getMaxPacketSize() / 32;
            //  int packSize=65530;
            byte[] buffer = new byte[packSize];
            int len = -1;

            while ((len = inputStream.read(buffer)) != -1)
            {
                byte[] temp = null;
                if (len == packSize)
                {
                    temp = buffer;
                } else
                {
                    temp = new byte[len];
                    System.arraycopy(buffer, 0, temp, 0, len);
                }
                BinaryPacket packet = new BinaryPacket();
                packet.packetId = ++packID;
                packet.data = temp;
                packet.write(backendIOConnection);
            }

        }
        finally
        {
            inputStream.close();
        }


        return  packID;
    }
}
