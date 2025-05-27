package com.whosly.avacita.driver;

/**
 * 自定义驱动前缀
 */
public class EncDriver extends org.apache.calcite.avatica.remote.Driver {
    static {
        new EncDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:enc:";
    }

}
