package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"

	ole "github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"github.com/postfinance/single"
	"github.com/spf13/viper"
)

func main() {
	f, err := os.OpenFile("Result.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	loadConfig()

	lockfile_name := "exchange_" + viper.GetString("dbname")
	lock, _ := single.New(lockfile_name)
	if err := lock.Lock(); err != nil {
		log.Println("Обмен уже идет! Отменяем новый вызов. Доп инфо:", err.Error(), "\r")
		log.Println("Lockfile:", lock.Lockfile(), "\r")
		panic("err.Error()")
	}
	log.Println("Начинаем обмены", "\r")

	if viper.GetBool("parallel") {
		if err := ole.CoInitializeEx(0, ole.COINIT_MULTITHREADED); err != nil {
			log.Println("Ошибка при вызове CoInitializeEx:", err.Error(), "\r")
			panic(err.Error())
		}
	} else {
		if err := ole.CoInitialize(0); err != nil {
			log.Println("Ошибка при вызове CoInitialize:", err.Error(), "\r")
			panic(err.Error())
		}
	}
	defer ole.CoUninitialize()

	checkAndUpdateCfg()

	pdbs := viper.GetStringSlice("pdb")

	if viper.GetBool("parallel") {
		var wg sync.WaitGroup
		for i := range pdbs {
			wg.Add(1)
			go Receive(pdbs[i], &wg)
		}
		wg.Wait()

		for i := range pdbs {
			wg.Add(1)
			go Send(pdbs[i], &wg)
		}
		wg.Wait()
	} else {
		o1C := Get1CObject()
		defer o1C.Release()
		for i := range pdbs {
			ReceiveSync(o1C, pdbs[i])
		}
		for i := range pdbs {
			SendSync(o1C, pdbs[i])
		}
	}

	Cleanup()

	if err := lock.Unlock(); err != nil {
		log.Println("Ошибка при разблокировке задачи, удалите exchange.lock файл вручную:", err.Error(), "\r")
		log.Println("Путь до файла:", lock.Lockfile(), "\r")
		panic("err.Error()")
	}
	log.Println("Завершен обмен\r")
}

func loadConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Println("Конфиг не загрузился, возможно он неправильно отформатирован:", err.Error(), "\r")
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
}

func Prepare1CObject() (unknown *ole.IUnknown) {
	var err error

	defer func() {
		if r := recover(); r != nil {
			RegisterCOM()
			unknown, err = oleutil.CreateObject("v83.COMConnector")
			if err != nil {
				log.Println("Не удалось повторно создать объект 1С:", err.Error(), "\r")
				panic(err.Error())
			}
		}
	}()

	unknown, err = oleutil.CreateObject("v83.COMConnector")
	if err != nil {
		log.Println("Не удалось создать объект 1С, пробуем зарегистрировать через regsvr32:", err.Error(), "\r")
		panic(err.Error())
	}

	return unknown
}

func Get1CObject() (obj *ole.IDispatch) {
	unknown := Prepare1CObject()
	o1C, err := unknown.QueryInterface(ole.IID_IDispatch)
	if err != nil {
		log.Println("Не удалось обработать объект 1С:", err.Error(), "\r")
		panic(err.Error())
	}
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"Srvr=%s;Ref=%s;Usr=%s;Pwd=%s",
		viper.GetString("server"),
		viper.GetString("dbname"),
		viper.GetString("login"),
		viper.GetString("password"),
	)

	connectionTo1C, err := oleutil.CallMethod(o1C, "Connect", b.String())
	if err != nil {
		log.Println("Не получилось подключиться к 1С:", err.Error(), "\r")
		panic(err.Error())
	}
	return connectionTo1C.ToIDispatch()
}

func checkAndUpdateCfg() {
	o1C := Get1CObject()
	defer o1C.Release()
	if cfgNeedsUpdate, err := oleutil.CallMethod(o1C, "Eval1C", "ПланыОбмена."+viper.GetString("exchange_name")+".ЭтотУзел().МетаданныеИзменились"); err != nil {
		log.Println("Не удалось проверить обновление:", err.Error(), "\r")
		panic(err.Error())
	} else {
		bNeedUpdate := cfgNeedsUpdate.Value().(bool)
		if bNeedUpdate {
			var b strings.Builder
			fmt.Fprintf(
				&b,
				"CONFIG /S\"%s\\%s\" /N\"%s\" /P\"%s\" /UpdateDBCfg /Out UpdCfg.log",
				viper.GetString("server"),
				viper.GetString("dbname"),
				viper.GetString("login"),
				viper.GetString("password"),
			)
			runcommand := b.String()
			defer func() {
				r := recover()
				if r != nil {
					log.Println("Ошибка при обновлении", r, "\r")
				}
			}()

			if err := exec.Command(viper.GetString("path_to_1c"), strings.Split(runcommand, " ")...).Run(); err != nil {
				log.Println("Не удалось обновить конфигурацию:", err.Error(), "\r")
			}
		}
	}
}

func Receive(code string, wg *sync.WaitGroup) {
	o1C := Get1CObject()
	defer o1C.Release()
	ReceiveSync(o1C, code)
	wg.Done()
}

func Send(code string, wg *sync.WaitGroup) {
	o1C := Get1CObject()
	defer o1C.Release()
	SendSync(o1C, code)
	wg.Done()
}

func ReceiveSync(o1C *ole.IDispatch, code string) {
	log.Println("Читаем пакет", code, "\r")
	command := "ПланыОбмена." + viper.GetString("exchange_name") + ".НайтиПоКоду(\"" + code + "\").ПолучитьОбъект().ПрочитатьИзменения()"
	if _, err := oleutil.CallMethod(o1C, "Eval1C", command); err != nil {
		log.Printf("ФЕЙЛ: Ошибка при принятии обмена с %s филиала: %s\n", code, err.Error())
	}
	log.Println("УСПЕХ: Прочитан пакет", code, "\r")
}

func SendSync(o1C *ole.IDispatch, code string) {
	if fromCode, err := oleutil.CallMethod(o1C, "Eval1C", "ПланыОбмена."+viper.GetString("exchange_name")+".ЭтотУзел().Код"); err != nil {
		log.Println("Не удалось получить код текущего узла:", err.Error(), "\r")
	} else if checkIfExchangeExists(fromCode.Value().(string), code) {
		log.Println("ИНФО: обмен из", fromCode.Value().(string), "в", code, "уже существует", "\r")
		return
	}
	log.Println("Отправляем пакет", code, "\r")
	command := "ПланыОбмена." + viper.GetString("exchange_name") + ".НайтиПоКоду(\"" + code + "\").ПолучитьОбъект().ЗаписатьИзменения()"
	if _, err := oleutil.CallMethod(o1C, "Execute1C", command); err != nil {
		log.Printf("ФЕЙЛ: Ошибка при отправке обмена в %s филиал: %s\n", code, err.Error())
	}
	log.Println("УСПЕХ: Отправлен пакет", code, "\r")
}

func RegisterCOM() {
	if err := exec.Command("regsvr32.exe", viper.GetString("path_to_com")).Run(); err != nil {
		log.Println("Не удалось зарегистрировать comcntr.dll:", err.Error(), "\r")
		panic(err.Error())
	}
}

func Cleanup() {
	fpdb := viper.GetStringSlice("fpdb")
	suffixes := viper.GetStringSlice("suffixes")
	for i := range fpdb {
		if files, err := ioutil.ReadDir(fpdb[i]); err != nil {
			log.Println("Ошибка при доступе к папке ", fpdb[i], ":", err.Error(), "\r")
		} else {
			for _, file := range files {
				for _, suffix := range suffixes {
					if strings.Contains(file.Name(), suffix) {
						if err := os.Remove(fpdb[i] + "\\" + file.Name()); err != nil {
							log.Println("Ошибка при удалении файла", file.Name(), ":", err.Error(), "\r")
						}
					}
				}
			}
		}
	}
	log.Println("Очистка прошла", "\r")
}

func checkIfExchangeExists(fromCode, toCode string) bool {
	fpdb := viper.GetStringSlice("fpdb")
	suffix := fromCode + "_" + toCode
	for i := range fpdb {
		if files, err := ioutil.ReadDir(fpdb[i]); err != nil {
			log.Println("Проверка пакета: ошибка при доступе к папке ", fpdb[i], ":", err.Error(), "\r")
		} else {
			for _, file := range files {
				if strings.Contains(file.Name(), suffix) && !file.IsDir() {
					return true
				}
			}
		}
	}
	return false
}
