package com.whosly.avacita.driver;

import org.apache.calcite.avatica.DriverVersion;

/**
 * 自定义驱动前缀
 */
public class EncDriver extends org.apache.calcite.avatica.remote.Driver {
    static {
        new EncDriver().register();
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(
                EncDriver.class,
                "org-apache-calcite-jdbc.properties",
                "Avatica EncRemote JDBC Driver",
                "unknown version",
                "AvaticaEnc",
                "unknown version");
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:enc:";
    }

}
