using System;
using System.Runtime.CompilerServices;

namespace SPH.PLAS.ESA
{
    public interface IStandardLog
    {
        void Verbose(string message);
        void Verbose(string messageTemplate, params object[] propertyValues);
        void Debug(string message);
        void Debug(string messageTemplate, params object[] propertyValues);
        void Debug<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);
        void Information(string message);
        void Information(string messageTemplate, params object[] propertyValues);
        void Information<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);
        void Warning(string message);
        void Warning(string messageTemplate, params object[] propertyValues);
        void Warning<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);
        void Error(string message);
        void Error(Exception e, string msg);
        void Error(string messageTemplate, params object[] propertyValues);
        void Error<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);
        void Fatal(string message);
        void Fatal(Exception e, string msg);
        void Fatal(string messageTemplate, params object[] propertyValues);
        void Fatal<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1);
        void WirteMsg(string message);
        void WirteLog(string message);
        void Exit();
    }
}