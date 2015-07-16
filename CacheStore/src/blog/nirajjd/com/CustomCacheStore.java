package blog.nirajjd.com;

import blog.nirajjd.com.constants.CachedObejct;
import com.oracle.coherence.patterns.pushreplication.PublishingCacheStore;
import com.tangosol.net.BackingMapManagerContext;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;
import com.tangosol.util.Binary;
import com.tangosol.util.BinaryEntry;
import com.tangosol.util.ExternalizableHelper;

/**
 * Created by Niraj on 7/15/2015.
 */
public class CustomCacheStore extends PublishingCacheStore {

    public CustomCacheStore(String cacheName) {
        super(cacheName);
    }

    //Update DB in cacheStore, using values with in objects stored in coherence to make sure DB call doesnt happen multiple time in active active cross datacenter environment.
    /*public synchronized void store(BinaryEntry entry) {
        String key = (String) getKeyFromBinaryEntry(entry);
        CachedObejct data = (CachedObejct) getValueFromBinaryEntry(entry);
        if(data.isDbUpdate()) {
            data.setDbUpdate(false);
            Binary updatedObject = getBinaryEntryFromValue(data,entry);
            BinaryEntry binaryEntry = new BackingMapBinaryEntry(entry.getBinaryKey(), updatedObject,updatedObject,entry.getContext());
            // write code to udpate DB here once done.

            //sending updated binary object so that db update doesnt happen in both data centers.
            super.store(binaryEntry);
        }


    }*/


    //Update DB in cacheStore, using custom cache to make sure DB call doesnt happen multiple time in active active cross datacenter environment.
    public synchronized void store(BinaryEntry entry) {
        String key = (String) getKeyFromBinaryEntry(entry);
        CachedObejct data = (CachedObejct) getValueFromBinaryEntry(entry);
        NamedCache dbUpdateCache = CacheFactory.getCache("YOUR CACHE NAME HERE");
        boolean dbUpdateBoolean = (boolean)dbUpdateCache.get("KEY OF THE OBEJCT YOU STORE IN CACHE");
        if(dbUpdateBoolean) {
            dbUpdateCache.put("KEY OF THE OBEJCT YOU STORE IN CACHE", false);

            // write code to udpate DB here once done.

            super.store(entry);
        }


    }

    public Object getKeyFromBinaryEntry(BinaryEntry entry){
        BackingMapManagerContext ctx = entry.getContext();
        return ExternalizableHelper.fromBinary(entry.getBinaryKey(), ctx.getCacheService().getSerializer());
    }
    public Object getValueFromBinaryEntry(BinaryEntry entry){
        BackingMapManagerContext ctx = entry.getContext();
        return ExternalizableHelper.fromBinary(entry.getBinaryValue(), ctx.getCacheService().getSerializer());
    }

    public Binary getBinaryEntryFromValue (Object obj, BinaryEntry entry){
        BackingMapManagerContext ctx = entry.getContext();
        return ExternalizableHelper.toBinary(obj,ctx.getCacheService().getSerializer());
    }
}
